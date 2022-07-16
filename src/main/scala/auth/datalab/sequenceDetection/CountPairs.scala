package auth.datalab.sequenceDetection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CountPairs {

  /**
   * Method to extract the precomputed counts for each pair of events
   *
   * @param data The [EventIdTimeLists] RDD with the pairs
   * @return A [CountList] class RDD with the counts for each event
   *         The return sum is the total number of milliseconds between the events
   *         of a pair
   */
  def createCountCombinationsRDD(data: RDD[Structs.EventIdTimeLists]): RDD[Structs.CountList] = {
    val spark = SparkSession.builder().getOrCreate()

    val counts = data
      .map(row => {
        val event1 = row.event1
        var event2 = row.event2
        var sum = 0L
        var count = 0
        //Pair
        event2 = row.event2
        row.times.foreach(r => {
          for (i <- 1 until r.times.length by 2) {
            val time1 = r.times(i - 1)
            val time2 = r.times(i)
            sum += Utils.getDifferenceTime(time1, time2)
            count += 1
          }
        })

        Structs.CountList(event1, List((event2, sum, count)))
      })
      .keyBy(r => r.event1_name)
      .reduceByKey((p1, p2) => {
        val newList = List.concat(p1.times, p2.times)
        Structs.CountList(p1.event1_name, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    counts
  }


  def createCountPairs(data: RDD[Structs.EventIdTimeLists]): RDD[(String, String, Long, Int)] = {
    data.map(row => {
      val event1 = row.event1
      var event2 = row.event2
      var sum = 0L
      var count = 0
      //Pair
      event2 = row.event2
      row.times.foreach(r => {
        for (i <- 1 until r.times.length by 2) {
          val time1 = r.times(i - 1)
          val time2 = r.times(i)
          sum += Utils.getDifferenceTime(time1, time2)
          count += 1
        }
      })
      (event1, event2, sum, count)
    })
  }

  def merge(data1: RDD[(String, String, Long, Int)], data2: RDD[(String, String, Long, Int)]): RDD[(String, String, Long, Int)] = {
    data2.keyBy(x => (x._1, x._2))
      .fullOuterJoin(data1.keyBy(x => (x._1, x._2)))
      .map(x => {
        val t1 = x._2._1.getOrElse(("", "", 0L, 0))._4
        val t2 = x._2._2.getOrElse(("", "", 0L, 0))._4
        val d1 = x._2._1.getOrElse(("", "", 0L, 0))._3
        val d2 = x._2._2.getOrElse(("", "", 0L, 0))._3
        val average_duration = (d1 * t1 + d2 * t2) / (t1 + t2)
        (x._1._1, x._1._2, average_duration, t1 + t2)
      })
  }

  def toCombinationRDD(data: RDD[(String, String, Long, Int)]): RDD[Structs.CountList] = {
    val spark = SparkSession.builder().getOrCreate()
    data.map(x => {
      Structs.CountList(x._1, List((x._2, x._3, x._4)))
    })
      .keyBy(r => r.event1_name)
      .reduceByKey((p1, p2) => {
        val newList = List.concat(p1.times, p2.times)
        Structs.CountList(p1.event1_name, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
  }

}
