package auth.datalab.sequenceDetection.PairExtraction

import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

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
      }).filter(x =>{
        x.times.head.times.nonEmpty
      })
    }).toList

  }

  private def createPairsIndexing(eventAtime: List[String], eventBtimes: List[String], sequenceid: Long): Structs.IdTimeList = {
    var posA = 0
    var posB = 0
    var prev = ""
    var response = Structs.IdTimeList(sequenceid, List[String]())
    while (posA < eventAtime.size && posB < eventBtimes.size) {
      if (Utils.compareTimes(eventAtime(posA), eventBtimes(posB))) { // goes in if a  b
        if (Utils.compareTimes(prev, eventAtime(posA))) {
          val newList = response.times :+ eventAtime(posA) :+ eventBtimes(posB)
          response = Structs.IdTimeList(response.id, newList)
          prev = eventBtimes(posB)
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
