package auth.datalab.sequenceDetection.PairExtraction

import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ZipCombinations {
  private val DELIMITER= "¦delab¦"

  /**
   * Method to extract the exact timestamps of each pair combination from a sequence of events
   *
   * @param data The RDD with the sequence for each user/device
   * @param tempTable The temp table name from which the method needs to read data from cassandra
   * @return An [EventIdTimeLists] class RDD with the all of the pairs and their correct timestamps
   * @note Needs more resources than the loose version because of the join
   */
  def zipCombinationsRDD(data: RDD[Structs.Sequence], tempTable: DataFrame, time: String, funnelDate: java.sql.Timestamp): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val combinations = data
      .flatMap(row => extractZipCombinations(row))
      .coalesce(spark.sparkContext.defaultParallelism)

    val keys = combinations.map(l => (l.event1, l.event2, l.id))
      .toDF("ev1", "ev2", "id")
      .coalesce(spark.sparkContext.defaultParallelism)

    val joined = keys
      .join(tempTable, Seq("ev1", "ev2", "id"), "left_outer")
      .rdd
      .map(l => (l.getString(0), l.getString(1), l.getString(2), l.getString(3)))
      .keyBy(l => (l._1, l._2, l._3))
      .repartitionAndSortWithinPartitions(new HashPartitioner(spark.sparkContext.defaultParallelism))

    val sortedCombinations = combinations
      .keyBy(l => (l.event1, l.event2, l.id))
      .repartitionAndSortWithinPartitions(new HashPartitioner(spark.sparkContext.defaultParallelism))

    val zippedCombinations = sortedCombinations
      .zip(joined)
      .map(l => {
        val row = (l._1._2.event1, l._1._2.event2, l._1._2.id, l._1._2.times, l._2._2._4)
        extractZipTimes(row)
      })
      .filter(_.times.nonEmpty)
      .coalesce(spark.sparkContext.defaultParallelism)
      .keyBy(p => (p.event1, p.event2))
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.EventIdTimeLists(a.event1, a.event2, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)

    zippedCombinations.count()
    tempTable.unpersist()
    zippedCombinations
  }

  /**
   * Method to extract the exact pairs of events along with the timestamps for each event from a sequence of events
   *
   * @param line A sequence of events from a user or device
   * @return A list with all of the pairs and their timestamps
   */
  private def extractZipCombinations(line: Structs.Sequence): List[Structs.JoinTemp] = {
    //only pairs
    var finalMap = mutable.HashMap[(String, String), List[String]]()
    //    val events = line.event
//    val events = line.event.asInstanceOf[List[GenericRowWithSchema]].map(ev => {
//      Structs.Event(ev.getString(0), ev.getString(1))
//    })
    val events = line.events

    //1st loop to compute the possible pairs/triplets
    for (i <- 0 until events.size - 1) { //1st loop
      val pairA = events(i)
      //create list A
      var occurrencesA: ListBuffer[String] = ListBuffer(pairA.event + this.DELIMITER + pairA.timestamp)
      for (y <- i + 1 until events.size) { //2nd loop
        val pairB = events(y)
        //Update list A
        if (pairB.event == pairA.event) occurrencesA += (pairB.event + this.DELIMITER + pairB.timestamp)
        if (!finalMap.contains((pairA.event, pairB.event))) {
          //create list B & AA
          var occurrencesB: ListBuffer[String] = ListBuffer()
          var occurrencesAA: ListBuffer[String] = ListBuffer()
          if (pairB.event != pairA.event) {
            occurrencesB += (pairB.event + this.DELIMITER + pairB.timestamp)
          }
          for (j <- y + 1 until events.size) { //3rd loop
            val pairC = events(j)
            //update list B & AA
            if (pairC.event == pairB.event) occurrencesB += (pairC.event + this.DELIMITER + pairC.timestamp)
            if (pairC.event == pairA.event && pairB.event != pairA.event) occurrencesAA += (pairC.event + this.DELIMITER + pairC.timestamp)
          } //end 3rd loop
          val finalOccurrences = List.concat(occurrencesA, occurrencesAA, occurrencesB)
            .sortWith((a, b) => Utils.sortByTime(a.split(this.DELIMITER)(1), b.split(this.DELIMITER)(1)))
          finalMap.+=(((pairA.event, pairB.event), finalOccurrences))
        }
      } //end 2nd loop
    } //end 1st loop

    val res = finalMap.toList.map(row => {
      val list = row._2
      Structs.JoinTemp(row._1._1, row._1._2, line.sequence_id.toString, list)
    })
    res
  }

  /**
   * Private method to extract the correct timestamps from each pair (without duplicates)
   *
   * @param line The pair and its timestamps for each event
   * @return The pair with the trimmed timestamps or an empty list
   */
  private def extractZipTimes(line: (String, String, String, List[String], String)): Structs.EventIdTimeLists = {
    val eventA = line._1
    val eventB = line._2
    val sequence_id = line._3
    val times = line._4
    val pr_time = line._5
    val max = 2
    val eventC = null

    var finalTimes = ListBuffer[String]()
    var count = 1
    for (elem <- times) {
      val event = elem.split(this.DELIMITER)(0)
      val eventTime = elem.split(this.DELIMITER)(1)
      if (count == 1) {
        if (event == eventA) {
          if (pr_time == null || Utils.compareTimes(pr_time, eventTime)) {
            finalTimes += eventTime
            count = 2
          }
        }
      } else if (count == 2) {
        if (event == eventB) {
          if (max == 3) count = 3
          else {
            count = 1
          }
          finalTimes += eventTime

        }
      } else {
        if (event == eventC) {
          finalTimes += eventTime
          count = 1
        }
      }
    }
    if (count == 2) {
      finalTimes = finalTimes.dropRight(1)
    } else if (count == 3) {
      finalTimes = finalTimes.dropRight(2)
    }
    if (finalTimes.nonEmpty)
      Structs.EventIdTimeLists(eventA, eventB, List(Structs.IdTimeList(sequence_id.toLong, finalTimes.toList)))
    else
      Structs.EventIdTimeLists(eventA, eventB, List())
  }
}
