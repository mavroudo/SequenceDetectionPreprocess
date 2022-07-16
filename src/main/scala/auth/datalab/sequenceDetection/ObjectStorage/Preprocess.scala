package auth.datalab.sequenceDetection.ObjectStorage


import auth.datalab.sequenceDetection.Structs.Sequence
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Preprocess {
  case class SequenceP(events: List[EventP], trace_id: Long)

  case class EventP(event_type: String, position: Long)

  def execute(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean): Long = {
    case class PairOccs(eventA: String, eventB: String, l: List[(Long, List[(Long, Long)])])
    val start = System.currentTimeMillis()
    val index_table: String = s"""s3a://siesta/$log_name/idx/"""
    val spark = SparkSession.builder().getOrCreate()

    val dataP = sequenceRDD.map(trace => {
      val events = trace.events.zipWithIndex.map(x => {
        EventP(x._1.event, x._2)
      })
      SequenceP(events, trace.sequence_id)
    })
    import spark.sqlContext.implicits._
    val combinations= dataP
      .flatMap(l => this.extractPairs(l))
      .groupBy(_.getPair)
      .map(pair => {
        val occs = pair._2.groupBy(_.trace)
          .map(x => {
            (x._1, x._2.map(y => (y.positionA, y.positionB)).toList)
          }).toList
        (pair._1._1, pair._1._2, occs)
      })

    val df = combinations.toDF("eventA","eventB","occurrences")
    df.show(10)

    if (overwrite) {
      df
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .mode("overwrite")
        .parquet(index_table)
    } else {
      df.repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .parquet(index_table)
    }

    System.currentTimeMillis() - start


  }

  private def extractPairs(s: SequenceP): List[Occurrence] = {
    var mapping = mutable.HashMap[String, List[Long]]()

    s.events.foreach(event => { //mapping events to positions
      val oldSequence = mapping.getOrElse(event.event_type, null)
      if (oldSequence == null) {
        mapping.+=((event.event_type, List(event.position)))
      } else {
        val newListPos = oldSequence :+ event.position
        mapping.+=((event.event_type, newListPos))
      }
    })
    mapping.flatMap(eventA => { //double for loop in distinct events
      mapping.flatMap(eventB => {
        this.createPairs(eventA._1, eventB._1, eventA._2, eventB._2, s.trace_id)
      })
    }).toList


  }

  private def createPairs(eventA: String, eventB: String, eventAtimes: List[Long], eventBtimes: List[Long], traceid: Long): List[Occurrence] = {
    var posA = 0
    var posB = 0
    var prev = -1L

    val tA: List[Long] = eventAtimes.sorted
    val tB: List[Long] = eventBtimes.sorted


    val response = new ListBuffer[Occurrence]
    while (posA < tA.size && posB < tB.size) {
      if (tA(posA) < tB(posB)) { // goes in if a  b
        if (prev < tA(posA)) {
          response += new Occurrence(eventA, eventB, traceid, tA(posA), tB(posB))
          prev = tB(posB)
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
}
