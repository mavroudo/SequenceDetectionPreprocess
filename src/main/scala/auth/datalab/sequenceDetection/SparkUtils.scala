package auth.datalab.sequenceDetection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable

object SparkUtils {


  /**
   * Method to extract the loose (some duplicates) combinations (pairs)
   * along with the timestamps of their events from a sequence of events
   *
   * @param data The [IdEventList] RDD
   * @return An RDD of [EventIdTimeLists] class that holds for each pair
   *         the timestamps on each device or user id
   */
  def createCombinationsRDD(data: RDD[Structs.Sequence], type_of_search: String): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()

    val combinations = data
      .flatMap(l => {
        if (type_of_search == "skip till match") {
          extractPairsSkippTillMatch(l)
        } else { //that means it is "next"
          extractPairsNext(l)
        }
      }) //get pairs for each user
      .keyBy(l => (l.event1, l.event2)) //combine common combinations of users
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.EventIdTimeLists(a.event1, a.event2, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations
  }

  /**
   * Private method for creating every combination of events by 2
   * from a sequence of events
   *
   * @param line A sequence of events of a user or device
   * @return The list of pairs along with the timestamps of each of their events
   */
  def extractPairsSkippTillMatch(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    var index = mutable.HashMap[(String, String), List[String]]()
    var checked = mutable.HashSet[String]()
    for (i <- 0 until line.events.size - 1) {
      var eventA: String = ""
      var timeA: String = ""
      try {
        eventA = line.events(i).event
        timeA = line.events(i).timestamp
      } catch {
        case _: Throwable => {
          eventA = line.events(i).event
          timeA = line.events(i).timestamp
        }
      }
      if (!checked.contains(eventA)) {
        var loop = mutable.HashSet[String]()
        var eventB: String = ""
        var timeB = ""
        for (y <- i + 1 until line.events.size) {
          try {
            eventB = line.events(y).event
            timeB = line.events(y).timestamp
          } catch {
            case ex: java.lang.ClassCastException => {
              eventB = line.events(i).event
              timeB = line.events(i).timestamp
            }
          }
          if (eventB == eventA) {
            //Then it is either the first event of pair or an edge case
            val oldEdge = index.getOrElse((eventA, eventB), null) //edge case
            if (oldEdge == null) index.+=(((eventA, eventB), List(timeB, timeA)))
            else {
              val newList = timeB :: oldEdge
              index.+=(((eventA, eventA), newList))
            }
            loop.foreach(r => {
              val old = index.getOrElse((eventA, r), null) //first event of pair
              if (old != null) {
                val newList = timeB :: old
                index.+=(((eventA, r), newList))
              }
            })
            loop.clear()
          } else if (!loop.contains(eventB)) {
            val old = index.getOrElse((eventA, eventB), null)
            if (old == null) {
              index.+=(((eventA, eventB), List(timeB, timeA)))
            } else {
              val newList = timeB :: old
              index.+=(((eventA, eventB), newList))
            }
            loop.+=(eventB)
          }
        }
        checked.+=(eventA)
      }
    }

    val res = index.toList.map(row => {
      var list = row._2
      if (list.length % 2 != 0) {
        list = list.drop(1)
      }
      Structs.EventIdTimeLists(row._1._1, row._1._2, List(Structs.IdTimeList(line.sequence_id, list.reverse)))
    })
    res
  }

  def extractPairsNext(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
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
