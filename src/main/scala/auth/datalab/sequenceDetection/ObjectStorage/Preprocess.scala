package auth.datalab.sequenceDetection.ObjectStorage


import auth.datalab.sequenceDetection.ObjectStorage.Storage.{SequenceTable, SingleTable}
import auth.datalab.sequenceDetection.Structs.{CountList, Event, Sequence}
import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, log}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Preprocess {
  case class SequenceP(events: List[EventP], trace_id: Long)

  case class EventP(event_type: String, position: Long, timestamp: String)

  var join: Boolean = false
  var overwrite: Boolean = false
  var splitted_dataset: Boolean = false

  def execute(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean, join: Boolean, splitted_dataset: Boolean): Long = {
    this.overwrite = overwrite
    this.join = join
    this.splitted_dataset = splitted_dataset
    val start = System.currentTimeMillis()

    SingleTable.writeTable(sequenceRDD,log_name,overwrite,join,splitted_dataset)
    val seqs = SequenceTable.writeTable(sequenceRDD, log_name, overwrite, join, splitted_dataset)








//    val sequences:RDD[Sequence]=this.getSequenceData(sequenceRDD,log_name)
//
//    sequences.persist(StorageLevel.MEMORY_AND_DISK)
//    this.writeSeq(sequences, log_name)
//
//
//    val combinations = this.extractOccurrences(sequences)
//      .groupBy(_.getPair)
//    combinations.persist(StorageLevel.MEMORY_AND_DISK)
//    sequences.unpersist()
//
//    this.writeIndex(combinations, log_name)
//
//    this.createCountTable(combinations, log_name)
//
//    combinations.unpersist()
    System.currentTimeMillis() - start


  }

  private def extractPairs(s: SequenceP): List[Occurrence] = {
    var mapping = mutable.HashMap[String, (List[Long], List[String])]()

    s.events.foreach(event => { //mapping events to positions
      val oldSequence = mapping.getOrElse(event.event_type, null)
      if (oldSequence == null) {
        mapping.+=((event.event_type, (List(event.position), List(event.timestamp))))
      } else {
        val newListPos = oldSequence._1 :+ event.position
        val newListTime = oldSequence._2 :+ event.timestamp
        mapping.+=((event.event_type, (newListPos, newListTime)))
      }
    })
    mapping.flatMap(eventA => { //double for loop in distinct events
      mapping.flatMap(eventB => {
        this.createPairs(eventA._1, eventB._1, eventA._2, eventB._2, s.trace_id)
      })
    }).toList

  }

  private def createPairs(eventA: String, eventB: String, eventAtimes: (List[Long], List[String]), eventBtimes: (List[Long], List[String]), traceid: Long): List[Occurrence] = {
    var posA = 0
    var posB = 0
    var prev = -1L

    val tA = eventAtimes._1.zip(eventAtimes._2).sortWith((x, y) => x._1 < y._1)
    val tB = eventBtimes._1.zip(eventBtimes._2).sortWith((x, y) => x._1 < y._1)


    val response = new ListBuffer[Occurrence]
    while (posA < tA.size && posB < tB.size) {
      if (tA(posA)._1 < tB(posB)._1) { // goes in if a  b
        if (prev < tA(posA)._1) {
          response += new Occurrence(eventA, eventB, traceid, tA(posA)._1, tB(posB)._1, tA(posA)._2, tB(posB)._2)
          prev = tB(posB)._1
          posA += 1
          posB += 1
        } else {
          posA += 1
        }
      } else {
        posB += 1
      }
    }
    response.toList
  }

  private def writeSeq(sequences: RDD[Structs.Sequence], log_name: String): Unit = {
    val seq_table: String = s"""s3a://siesta/$log_name/seq/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._

    val df = sequences.map(x => {
      (x.sequence_id, x.events.map(y => (y.event, y.timestamp)))
    }).toDF("trace_id", "events")


    if (this.overwrite) {
      df.sort("trace_id")
        .write
        .mode("overwrite")
        .parquet(seq_table)
    } else if (this.join) {
      val dfPrev = spark.read.parquet(seq_table)
      df.withColumnRenamed("events", "newEvents")
        .join(dfPrev, "trace_id")
        .rdd.map(x => {
        val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
        val nEvents = x.getAs[Seq[Row]]("newEvents").map(y => (y.getString(0), y.getString(1)))
        (x.getAs[Long]("trace_id"), Seq.concat(pEvents, nEvents))
      }).toDF("trace_id", "events")
        .sort("trace_id")
        .write
        .mode("overwrite")
        .parquet(seq_table)
    } else if (this.splitted_dataset) {
      df.sort("trace_id")
        .write
        .mode("append")
        .parquet(seq_table)
    } else {
      df.sort("trace_id")
        .write
        .parquet(seq_table)
    }

  }

  private def extractOccurrences(sequences: RDD[Structs.Sequence]): RDD[Occurrence] = {
    val dataP = sequences.map(trace => {
      val events = trace.events.zipWithIndex.map(x => {
        EventP(x._1.event, x._2, x._1.timestamp)
      })
      SequenceP(events, trace.sequence_id)
    })

    dataP
      .flatMap(l => this.extractPairs(l))
  }

  private def writeIndex(comb: RDD[((String, String), Iterable[Occurrence])], log_name: String): Unit = {
    val index_table: String = s"""s3a://siesta/$log_name/idx/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val combinations = comb
      .map(pair => {
        val occs: List[(Long, List[(Long, Long)])] = pair._2.groupBy(_.trace)
          .map(x => {
            (x._1, x._2.map(y => (y.pA, y.pB)).toList)
          }).toList
        (pair._1._1, pair._1._2, occs)
      })
    val df = combinations.toDF("eventA", "eventB", "occurrences")

    if (this.overwrite) { //remove the previous and overwrite
      df
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .mode("overwrite")
        .parquet(index_table)
    } else if (this.splitted_dataset || this.join) { //combine with the previous and rewrite
      val dfPrev = spark.read.parquet(index_table)
      df.withColumnRenamed("occurrences", "newOccurrences")
        .join(right = dfPrev, usingColumns = Seq("eventA", "eventB"), joinType = "full")
        .rdd.map(x => {
        val nOccs: Seq[(Long, Seq[(Long, Long)])] = x.getAs[Seq[Row]]("newOccurrences").map(y => {
          (y.getLong(0), y.getAs[Seq[Row]](1).map(z => (z.getLong(0), z.getLong(1))))
        })
        val pOccs: Seq[(Long, Seq[(Long, Long)])] = x.getAs[Seq[Row]]("occurrences").map(y => {
          (y.getLong(0), y.getAs[Seq[Row]](1).map(z => (z.getLong(0), z.getLong(1))))
        })
        (x.getAs[String]("eventA"), x.getAs[String]("eventB"), Seq.concat(pOccs, nOccs))
      }).toDF("eventA", "eventB", "occurrences")
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .mode(SaveMode.Overwrite)
        .parquet(index_table)
    } else { // if no condition simply write
      df
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .parquet(index_table)
    }

  }

  private def createCountTable(comb: RDD[((String, String), Iterable[Occurrence])], log_name: String): Unit = {
    val count_table: String = s"""s3a://siesta/$log_name/count/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._

    val countList: RDD[(String, String, Long, Int)] = comb.map(x => {
      var l = 0L
      var t = 0
      x._2.foreach(oc => {
        t += 1
        l += Utils.getDifferenceTime(oc.tA, oc.tB)
      })
      (x._1._1, x._1._2, l / t, t)
    })

    if (overwrite) {
      countList
        .groupBy(_._1)
        .map(x => {
          val l = x._2.map(t => (t._2, t._3, t._4))
          CountList(x._1, l.toList)
        }).toDF("event", "times")
        .repartition(col("event"))
        .write
        .partitionBy("event")
        .mode("overwrite")
        .parquet(count_table)
    } else if (this.splitted_dataset || this.join) {
      val prev: RDD[((String, String), Long, Int)] = spark.read.parquet(count_table)
        .flatMap(row => {
          val eventA: String = row.getAs[String]("event")
          row.getAs[Seq[Row]]("times").map(y => {
            ((eventA, y.getString(0)), y.getLong(1), y.getInt(2))
          })
        }).rdd
      countList
        .map(x => ((x._1, x._2), x._3, x._4))
        .keyBy(_._1)
        .fullOuterJoin(prev.keyBy(_._1))
        .map(x => {
          val t1 = x._2._1.getOrElse(("", 0L, 0))._3
          val t2 = x._2._2.getOrElse(("", 0L, 0))._3
          val d1 = x._2._1.getOrElse(("", 0L, 0))._2
          val d2 = x._2._2.getOrElse(("", 0L, 0))._2
          val average_duration = (d1 * t1 + d2 * t2) / (t1 + t2)
          (x._1._1, x._1._2, average_duration, t1 + t2)
        })
        .keyBy(_._1)
        .groupByKey()
        .map(x => {
          val l = x._2.map(t => (t._2, t._3, t._4))
          CountList(x._1, l.toList)
        }).toDF("event", "times")
        .repartition(col("event"))
        .write
        .partitionBy("event")
        .mode(SaveMode.Overwrite)
        .parquet(count_table)
    } else {
      countList
        .groupBy(_._1)
        .map(x => {
          val l = x._2.map(t => (t._2, t._3, t._4))
          CountList(x._1, l.toList)
        }).toDF("event", "times")
        .repartition(col("event"))
        .write
        .partitionBy("event")
        .parquet(count_table)
    }

  }

  private def getSequenceData(sequences:RDD[Sequence],log_name:String):RDD[Sequence]={
    val spark = SparkSession.builder().getOrCreate()
    val seq_table: String = s"""s3a://siesta/$log_name/seq/"""
    import spark.sqlContext.implicits._
    if(this.join){ //combine sequences with the previous one to create
      val df = sequences.map(x => {
        (x.sequence_id, x.events.map(y => (y.event, y.timestamp)))
      }).toDF("trace_id", "events")
      val dfPrev = spark.read.parquet(seq_table)
      df.withColumnRenamed("events", "newEvents")
        .join(dfPrev, "trace_id")
        .rdd.map(x => {
        val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
        val nEvents = x.getAs[Seq[Row]]("newEvents").map(y => (y.getString(0), y.getString(1)))
        (x.getAs[Long]("trace_id"), Seq.concat(pEvents, nEvents))
      }).toDF("trace_id", "events")
        .map(row=>{
          val events = row.getAs[Seq[Row]]("events").map(x=>{
            Event(x.getString(0),x.getString(1))
          })
          Sequence(events.toList,row.getLong(1))
        }).rdd
    }else{
      sequences
    }
  }
}
