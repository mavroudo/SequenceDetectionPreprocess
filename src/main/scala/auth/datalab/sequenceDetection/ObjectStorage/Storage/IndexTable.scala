package auth.datalab.sequenceDetection.ObjectStorage.Storage

import auth.datalab.sequenceDetection.ObjectStorage.Occurrence
import auth.datalab.sequenceDetection.Structs
import auth.datalab.sequenceDetection.Structs.Sequence
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.net.{URI, URL}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object IndexTable {
  case class SequenceP(events: List[EventP], trace_id: Long)

  case class EventP(event_type: String, position: Long, timestamp: String)
  private var firstTime:Boolean=true

  /**
   * Writes the data to the file and returns the new created pairs
   *
   * @param sequenceRDD
   * @param log_name
   * @param overwrite
   * @param join
   * @param splitted_dataset
   * @return the newly created pairs
   */
  def writeTable(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean, join: Boolean, splitted_dataset: Boolean): RDD[((String, String), Iterable[Occurrence])] = {
    Logger.getLogger("idx_table").log(Level.INFO, "Start writing index table...")
    val index_table: String = s"""s3a://siesta/$log_name/idx/"""
    val spark = SparkSession.builder().getOrCreate()
    if (overwrite && firstTime) {
      val fs =FileSystem.get(new URI("s3a://siesta/"),spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(index_table), true)
      firstTime=false
    }
    val index = { //calculate the inverted pairs and in case of join remove any duplicates that are already stored
      val temp = this.extractOccurrences(sequenceRDD).groupBy(_.getPair)
      if (join) {
        this.removeDuplicatePairs(temp)
      } else {
        temp
      }
    }
    // store the new pairs generated. In case of splitted_dataset or join, merge the pairs with the previous stored
    // remind that for join we have already remove any previous
    val df = {
      if (splitted_dataset || join) { //combine with the previous values
        this.combine(this.transformToDF(index), index_table)
      } else {
        this.transformToDF(index)
      }
    }
    val mode = if (!splitted_dataset && !join) SaveMode.ErrorIfExists else SaveMode.Overwrite
    this.simpleWrite(df, mode, index_table)
    // return the index cal
    index

  }

  private def transformToDF(index: RDD[((String, String), Iterable[Occurrence])]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    index
      .map(pair => {
        val occs: List[(Long, List[(Long, Long)])] = pair._2.groupBy(_.trace)
          .map(x => {
            (x._1, x._2.map(y => (y.pA, y.pB)).toList)
          }).toList
        (pair._1._1, pair._1._2, occs)
      })
      .toDF("eventA", "eventB", "occurrences")
  }

  private def combine(df: DataFrame, index_table: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    try {
      val dfPrev = spark.read.parquet(index_table)
      val combined =
        df.withColumnRenamed("occurrences", "newOccurrences")
          .join(right = dfPrev, usingColumns = Seq("eventA", "eventB"), joinType = "full")
      combined
        .rdd.map(x => {
        val nOccs: Seq[(Long, Seq[(Long, Long)])] = x.getAs[Seq[Row]]("newOccurrences").map(y => {
          (y.getLong(0), y.getAs[Seq[Row]](1).map(z => (z.getLong(0), z.getLong(1))))
        })
        val pOccs: Seq[(Long, Seq[(Long, Long)])] = x.getAs[Seq[Row]]("occurrences").map(y => {
          (y.getLong(0), y.getAs[Seq[Row]](1).map(z => (z.getLong(0), z.getLong(1))))
        })
        (x.getAs[String]("eventA"), x.getAs[String]("eventB"), pOccs ++ nOccs)
      }).toDF("eventA", "eventB", "occurrences")
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        df
      case t: java.lang.NullPointerException =>
        t.printStackTrace()
        df
    }
  }

  private def simpleWrite(df: DataFrame, mode: SaveMode, index_table: String): Unit = {
    try {
      df
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .mode(mode)
        .parquet(index_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        Logger.getLogger("idx_table").log(Level.WARN, "Couldn't find table, so simply writing it")
        val mode2 = if (mode == SaveMode.Overwrite) SaveMode.ErrorIfExists else SaveMode.Overwrite
        df
          .repartition(col("eventA"))
          .write
          .partitionBy("eventA")
          .mode(mode2)
          .parquet(index_table)
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

  private def createPairs(eventA: String, eventB: String, eventAtimes: (List[Long], List[String]),
                          eventBtimes: (List[Long], List[String]), traceid: Long): List[Occurrence] = {
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

  private def removeDuplicatePairs(index: RDD[((String, String), Iterable[Occurrence])]): RDD[((String, String), Iterable[Occurrence])] = {
    //TODO: when implement join implement also this one to remove any duplicate pairs
    index
  }


}
