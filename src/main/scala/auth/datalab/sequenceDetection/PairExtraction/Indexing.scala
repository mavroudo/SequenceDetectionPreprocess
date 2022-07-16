package auth.datalab.sequenceDetection.PairExtraction


import java.util.Collections
import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * Extract pairs using Indexing method
 */
object Indexing extends ExtractPairs {

  override def extract(data: RDD[Structs.Sequence]): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    val combinations = data
      .flatMap(l => this.indexingMethodPairs(l))
      .keyBy(l => (l.event1, l.event2))
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.EventIdTimeLists(a.event1, a.event2, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations
  }

  private def indexingMethodPairs(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
    var mapping = HashMap[String, List[String]]()
    val sequence_id = line.sequence_id
    line.events.foreach(event => { //This will do the mapping
      val oldSequence = mapping.getOrElse(event.event, null)
      if (oldSequence == null) {
        mapping.+=((event.event, List(event.timestamp)))
      } else {
        val newList = oldSequence :+ event.timestamp
        mapping.+=((event.event, newList))
      }
    })
    mapping.flatMap(eventA => { //double for loop in distinct events
      mapping.map(eventB => {
        val list = List[Structs.IdTimeList]() :+ this.createPairsIndexing(eventA._2, eventB._2, sequence_id)
        Structs.EventIdTimeLists(eventA._1, eventB._1, list)
      }).filter(x => {
        x.times.head.times.nonEmpty
      })
    }) toList
  }

  private def indexingMethodPairs2(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
    //      var mapping = HashMap[String, List[String]]()
    val sequence_id = line.sequence_id
    var mapping = line.events
      .groupBy(_.event)
      .map(x => {
        (x._1, x._2.map(_.timestamp).sortWith(Utils.compareTimes))
      })
    val l: ListBuffer[Structs.EventIdTimeLists] = new ListBuffer[Structs.EventIdTimeLists]()
    mapping.foreach(x1 => {
      mapping.foreach(x2 => {
        val list = List[Structs.IdTimeList]() :+ this.createPairsIndexing(x1._2, x2._2, sequence_id)
        l += Structs.EventIdTimeLists(x1._1, x2._1, list)
      })
    })
    l.toList.filter(_.times.head.times.nonEmpty)
  }

  private def createPairsIndexing(eventAtimes: List[String], eventBtimes: List[String], sequenceid: Long): Structs.IdTimeList = {
    var posA = 0
    var posB = 0
    var prev = ""

    val sortedA = eventAtimes.sortWith(Utils.compareTimes)
    val sortedB = eventBtimes.sortWith(Utils.compareTimes)
    var response = Structs.IdTimeList(sequenceid, List[String]())
    while (posA < sortedA.size && posB < sortedB.size) {
      if (Utils.compareTimes(sortedA(posA), sortedB(posB))) { // goes in if a  b
        if (Utils.compareTimes(prev, sortedA(posA))) {
          val newList = response.times :+ sortedA(posA) :+ sortedB(posB)
          response = Structs.IdTimeList(response.id, newList)
          prev = sortedB(posB)
          posA += 1
          posB += 1
        } else {
          posA += 1
        }
      } else {
        posB += 1
      }
    }
    response

  }


}
