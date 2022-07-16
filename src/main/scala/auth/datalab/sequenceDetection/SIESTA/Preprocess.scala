package auth.datalab.sequenceDetection.SIESTA

import auth.datalab.sequenceDetection.CommandLineParser.Config
import auth.datalab.sequenceDetection.PairExtraction.{Indexing, Parsing, State, StrictContiguity, TimeCombinations, ZipCombinations}
import auth.datalab.sequenceDetection.{CountPairs, Structs, Utils}
import auth.datalab.sequenceDetection.Structs.InvertedOne
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object Preprocess {

  def preprocess(sequencesRDD: RDD[Structs.Sequence],
                 cass:CassandraConnection,table_name:String,
                 c:Config):Long={
    val table_temp = table_name + "_temp"
    val table_seq = table_name + "_seq"
    val table_idx = table_name + "_idx"
    val table_count = table_name + "_count"
    val table_single = table_name + "_one"
    val start = System.currentTimeMillis()
    createSingle(sequencesRDD, table_single, cass)
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val sequenceCombinedRDD: RDD[Structs.Sequence] = this.combine_sequences(sequencesRDD, table_seq, cass,
      timestamp, 10)
    val combinationsRDD = startCombinationsRDD(sequenceCombinedRDD, table_temp, "", c.join,
      c.algorithm, table_seq,0, cass)
    val combinationsCountRDD = CountPairs.createCountCombinationsRDD(combinationsRDD)
    cass.writeTableSequenceIndex(combinationsRDD, table_idx)
    cass.writeTableSeqCount(combinationsCountRDD, table_count)
    if (c.join) {
      cass.writeTableSeqTemp(combinationsRDD, table_temp)
    }
    combinationsRDD.unpersist()
    cass.writeTableSeq(sequenceCombinedRDD, table_seq)
    sequenceCombinedRDD.unpersist()
    sequencesRDD.unpersist()
    System.currentTimeMillis() - start
  }

  def createSingle(sequencesRDD:RDD[Structs.Sequence],table_single:String,cass:CassandraConnection):Unit={
    val invertedOneRDD: RDD[Structs.InvertedOne] = sequencesRDD.flatMap(x => {
      val id = x.sequence_id
      x.events.map(x => ((id, x.event), x))
    })
      .groupBy(_._1)
      .map(x => {
        val events = x._2.toList.map(_._2.timestamp).sortWith((a, b) => Utils.compareTimes(a, b))
        (x._1._2, Structs.IdTimeList(x._1._1, events))
      })
      .groupBy(_._1)
      .filter(_._2.toList.nonEmpty)
      .map(m => {
        InvertedOne(m._1, m._2.toList.map(_._2))
      })
    invertedOneRDD.persist(StorageLevel.MEMORY_AND_DISK)
    cass.writeTableOne(invertedOneRDD,table_single)
    invertedOneRDD.unpersist()
  }




  def createCombinationsRDD(seqRDD: RDD[Structs.Sequence], type_of_algorithm: String): RDD[Structs.EventIdTimeLists] = {
    type_of_algorithm match {
      case "parsing" => Parsing.extract(seqRDD)
      case "indexing" => Indexing.extract(seqRDD)
      case "state" => State.extract(seqRDD)
      case "strict" => StrictContiguity.extract(seqRDD)
      case _ => throw new Exception("Wrong type of algorithm")
    }
  }

  /**
   * Method to create the RDD with the sequence of events for sequence identifier by combining
   * the new data along with the already written ones to cassandra
   *
   * @param table_name The table from which we will collect data
   * @param timestamp  The timestamp to help with the days it needs to look back into
   * @param look_back  The maximum days (along with the new one) that the sequence must hold
   * @return An RDD of [Sequence] class with the data ready to be written to cassandra
   */
  def combine_sequences(seq_log_RDD: RDD[Structs.Sequence], table_name: String, cassandraConnection: CassandraConnection, timestamp: String, look_back: Int): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    seq_log_RDD.coalesce(spark.sparkContext.defaultParallelism)
    val funnel_time = Timestamp.valueOf(timestamp).getTime - (look_back * 24 * 3600 * 1000)
    val funnel_date = new Timestamp(funnel_time)
    val cassandraTable = cassandraConnection.readTable(table_name)
      .map(row => {
        val events = row
          .getAs[mutable.WrappedArray[String]](1)
          .toList
          .map(line => {
            val data = line
              .replace("Event(", "")
              .replace(")", "")
              .split(',')
            Structs.Event(data(0), data(1))
          })
          .filter(det => Utils.compareTimes(funnel_date.toString, det.timestamp)) //the events that are after the funnel time
        Structs.Sequence(events = events, sequence_id = row.getString(0).toLong)
      })
      .rdd
      .persist(StorageLevel.DISK_ONLY)
    cassandraTable.count()
    val res = this.mergeSeq(seq_log_RDD, cassandraTable)
      .coalesce(spark.sparkContext.defaultParallelism)
    res.count()
    cassandraTable.unpersist()
    res
  }

  /**
   * Method to merge 2 rdds of sequences
   *
   * @param newRdd The new sequence RDD
   * @param oldRdd The old sequence RDD
   * @return The merged RDD
   */
  def mergeSeq(newRdd: RDD[Structs.Sequence], oldRdd: RDD[Structs.Sequence]): RDD[Structs.Sequence] = {
    val tmp = oldRdd.union(newRdd)
    val finalCounts = tmp
      .keyBy(_.sequence_id)
      .reduceByKey((p1, p2) => {
        val newList = List.concat(p1.events, p2.events)
        Structs.Sequence(newList, p1.sequence_id)
      })
      .map(_._2)
    finalCounts

  }


  def startCombinationsRDD(seqRDD: RDD[Structs.Sequence], table_temp: String,
                           time: String, join: Boolean, type_of_algorithm: String,
                           table_name: String, look_back_hours: Int,
                           cassandraConnection: CassandraConnection): RDD[Structs.EventIdTimeLists] = {
    var res: RDD[Structs.EventIdTimeLists] = null
    if (!join) { // we have no prio knowledge and it will not have next
      res = createCombinationsRDD(seqRDD, type_of_algorithm)
      res = TimeCombinations.timeCombinationsRDD(res, time) // we need to eliminate all the pairs completed before the time
    } else {
      val funnel_time = Timestamp.valueOf("2000-01-01 00:00:00").getTime - (look_back_hours * 3600 * 1000)
      val funnel_date = new Timestamp(funnel_time)
      val tempTable: DataFrame = cassandraConnection.readTemp(table_temp, funnel_date)
      res = ZipCombinations.zipCombinationsRDD(seqRDD, tempTable, table_name, funnel_date)
    }
    res
  }



}
