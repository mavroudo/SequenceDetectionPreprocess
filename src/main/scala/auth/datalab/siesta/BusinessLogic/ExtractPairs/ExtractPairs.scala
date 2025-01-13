package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

object ExtractPairs {

  /**
   * Extracts the event type pairs from an RDD that contains the complete traces. If there are previously indexed pairs
   * this process will only calculate the new event type pairs by utilizing the information stored in the LastChecked
   * table. Additionally, there will be no event type pair where the two events will have greater time difference than
   * the one described by the parameter lookback.
   *
   * The generated pairs follow a Skip-till-next-match policy without overlapping in time. That is, in order to be two
   * occurrences of the same event type pair in a trace, the second one must start after the first one has ended.
   * For example: the trace t,,1,, = (a,b,c,a,b) contains 2 occurrences of the event type (a,b). However, the trace
   * t,,2,, = (a,a,c,b,b) only contains one occurrence of the previous event type pair.
   *
   * @param singleRDD    The RDD that contains the complete single inverted index
   * @param last_checked The loaded values from the LastChecked table (it should be null if it is the first time that
   *                     events are indexed in this log database).
   * @param lookback     The parameter that describes the maximum time difference that two events can have in order to
   *                     create an event type pair.
   * @return Two RDDs: (1) The first one that contains all the extracted event type pairs and (2) the second one contains
   *         the last timestamp of each event type pair for each trace.
   */
  def extract(singleRDD: RDD[Structs.InvertedSingleFull], last_checked: RDD[Structs.LastChecked],
              lookback: Int): (RDD[Structs.PairFull], RDD[Structs.LastChecked]) = {
    val spark = SparkSession.builder().getOrCreate()

    Logger.getLogger("Pair Extraction")
      .log(Level.INFO, s"Number of input traces ${singleRDD.map(_.id).count()} ")
    Logger.getLogger("Pair Extraction")
      .log(Level.INFO, s"Number of unique input traces ${singleRDD.map(_.id).distinct().count()}")


    val full = if (last_checked == null) {
      singleRDD.groupBy(_.id)
        .map(x => {
          this.calculate_pairs_stnm(x._2, null, lookback)
        })
    } else {
      singleRDD.groupBy(_.id).leftOuterJoin(last_checked.groupBy(_.id))
        .map(x => {
          val last = x._2._2.orNull
          this.calculate_pairs_stnm(x._2._1, last, lookback)
        })
    }
    val pairs = full.flatMap(_._1)
    val last_checked_pairs = full.flatMap(_._2)
    Logger.getLogger("Pair Extraction").log(Level.INFO, s"Extracted ${pairs.count()} event pairs")
    Logger.getLogger("Pair Extraction").log(Level.INFO, s"Extracted ${last_checked_pairs.count()} last checked")
    (pairs, last_checked_pairs)

  }

  /**
   * Extract the event type pairs from the single inverted index
   *
   * @param single    The complete single inverted index
   * @param last      The list with all the last timestamps that correspond to this event
   * @param lookback  The parameter that describes the maximum time difference between two events in a pair
   * @return A tuple, where the first element is the extracted event type pairs and the second element is the last
   *         timestamps for each event type.
   */
  private def calculate_pairs_stnm(single: Iterable[Structs.InvertedSingleFull], last: Iterable[Structs.LastChecked],
                                   lookback: Int): (List[Structs.PairFull], List[Structs.LastChecked]) = {
    val trace_id = single.head.id
    val singleMap: Map[String, Iterable[Structs.InvertedSingleFull]] = single.groupBy(_.event_name)
    val newLastChecked = new ListBuffer[Structs.PairFull]()
    val lastMap = if (last != null) last.groupBy(x => (x.eventA, x.eventB)) else null
    val all_events = single.map(_.event_name).toList.distinct
    val combinations = this.findCombinations(all_events)
    val results: ListBuffer[Structs.PairFull] = new ListBuffer[Structs.PairFull]()
    combinations.foreach(key=>{//for each combination
      //extract the timestamp and positions of the event types that described in the combination
      val ts1 = singleMap.getOrElse(key._1, null).map(x => x.times.zip(x.positions)).head
      val ts2 = singleMap.getOrElse(key._2, null).map(x => x.times.zip(x.positions)).head
      val last_checked = try {
        lastMap.getOrElse(key, null).head
      } catch {
        case _: Exception => null
      }
      //detects all the occurrences of this event type pair using the ts1 and ts2
      val nres = this.createTuples(key._1, key._2, ts1, ts2, lookback, last_checked, trace_id)
      //if there are any append them and also keep the last timestamp that they occurred
      if (nres.nonEmpty) {
        newLastChecked += nres.last
        results ++= nres
      }
    })
    val l = newLastChecked.map(x => {
      Structs.LastChecked(x.eventA, x.eventB, x.id, timestamp = x.timeB.toString)
    })
    //returns the two lists
    (results.toList, l.toList)
  }

  def createTuples(key1: String, key2: String, ts1: List[(String, Int)], ts2: List[(String, Int)],
                   lookback: Int, last_checked: Structs.LastChecked,
                   trace_id: String): List[Structs.PairFull] = {
    val pairs = new ListBuffer[Structs.PairFull]
    val lookbackMillis = Duration.ofDays(lookback).toMillis;
    var prev: Timestamp = null
    var i=0
    for (ea <- ts1) {
      val ea_ts = Timestamp.valueOf(ea._1)
      if ((prev == null || !ea_ts.before(prev)) && //evaluate based on previous
        (last_checked == null || !ea_ts.before(Timestamp.valueOf(last_checked.timestamp)))) { // evaluate based on last checked
        var stop = false
        while (i < ts2.size && !stop) { //iterate through what remained of the second list
          val eb = ts2(i)
          val eb_ts = Timestamp.valueOf(eb._1)
          if (ea._2 >= eb._2) { // if the event a is not before the event b we remove it
            i+=1
          } else { // if it is we create a new pair and remove it from consideration in the next iteration
            if (eb_ts.getTime - ea_ts.getTime <= lookbackMillis) { //evaluate lookback
              pairs.append(Structs.PairFull(key1, key2, trace_id, ea_ts, eb_ts, ea._2, eb._2))
              prev = eb_ts
              i+=1 //remove the event anyway and stop the process because the next events timestamps will be greater
            }
            //in case lookcback is not satisfied it will iterate back and evaluate the next timestamp from the first list
            //as a pair with this event
            stop = true
          }
        }
      }
    }
    pairs.toList
  }

  /**
   * Extracts all the possible event type pairs that can occur in a trace based on the unique event types
   *
   * @param event_types The unique event types in this trace
   * @return The possible event type pairs that can occur in this trace
   */
  private def findCombinations(event_types: List[String]): List[(String, String)] = {
    event_types.flatMap(t1 => {
      event_types.map(t2 => {
        (t1, t2)
      })
    }).distinct
  }

}
