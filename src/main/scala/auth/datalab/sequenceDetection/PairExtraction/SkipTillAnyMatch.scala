package auth.datalab.sequenceDetection.PairExtraction
import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SkipTillAnyMatch extends ExtractPairs {
  override def extract(data: RDD[Structs.Sequence]): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    val combinations = data
      .flatMap(l=>{
        skipTillAnyMatchPairs(l)
      })
      .keyBy(l=>(l.event1,l.event2))
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.EventIdTimeLists(a.event1, a.event2, newList)
      }).filter(a=>{
      a._2.times.nonEmpty
    })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations
  }

  private def skipTillAnyMatchPairs(line:Structs.Sequence):List[Structs.EventIdTimeLists] = {
    val sequence_id = line.sequence_id
    var index = mutable.HashMap[(String, String), List[String]]()
    for(i <- 0 until line.events.size-1){
      for (j <- i until line.events.size){
        val eventA = line.events(i).event
        val eventB = line.events(j).event
        val timeA = line.events(i).timestamp
        val timeB = line.events(j).timestamp
        val old = index.getOrElse((eventA, eventB), null)
        if (old == null) {
          index.+=(((eventA, eventB), List(timeA, timeB)))
        } else {
          val newList = timeA :: timeB :: old
          index.+=(((eventA, eventB), newList))
        }
      }
    }
    val res = index.toList.map(row => {
      Structs.EventIdTimeLists(row._1._1, row._1._2, List(Structs.IdTimeList(line.sequence_id, row._2)))
    })
    res
  }
}
