package auth.datalab.sequenceDetection.PairExtraction
import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object StrictContiguity extends ExtractPairs {
  override def extract(data: RDD[Structs.Sequence]): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    val combinations = data
      .flatMap(l => {
        extractPairsNext(l)
      })
      .keyBy(l => (l.event1, l.event2)) //combine common combinations of users
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

  /**
   * Private method for extracting pairs with "next" O(n)
   * @param line
   * @return
   */
  private def extractPairsNext(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
    var index = mutable.HashMap[(String, String), List[String]]()
    for (i <- 0 until line.events.size - 1) {
      val eventA = line.events(i).event
      val eventB = line.events(i + 1).event
      val timeA = line.events(i).timestamp
      val timeB = line.events(i + 1).timestamp
      val old = index.getOrElse((eventA, eventB), null)
      if (old == null) {
        index.+=(((eventA, eventB), List(timeA, timeB)))
      } else {
        val newList = timeA :: timeB :: old
        index.+=(((eventA, eventB), newList))
      }
    }

    val res = index.toList.map(row => {
      var list = row._2
      Structs.EventIdTimeLists(row._1._1, row._1._2, List(Structs.IdTimeList(line.sequence_id, row._2)))
    })
    res

  }
}
