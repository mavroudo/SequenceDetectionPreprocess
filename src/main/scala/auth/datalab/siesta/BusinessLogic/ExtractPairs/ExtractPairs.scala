package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

/**
 * This class describes how the event type pairs are extracted from the traces.
 */
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
   * @param singleRDD The RDD that contains the complete single inverted index
   * @param last_checked The loaded values from the LastChecked table (it should be null if it is the first time that
   *                     events are indexed in this log database).
   * @param intervals The list of the intervals
   * @param lookback The parameter that describes the maximum time difference that two events can have in order to
   *                 create an event type pair.
   * @return Two RDDs: (1) The first one that contains all the extracted event type pairs and (2) the second one contains
   *         the last timestamp of each event type pair for each trace.
   */
  def extract(singleRDD: RDD[Structs.InvertedSingleFull], last_checked: RDD[Structs.LastChecked],
              intervals: List[Structs.Interval], lookback: Int):(RDD[Structs.PairFull],RDD[Structs.LastChecked]) = {

    val spark = SparkSession.builder().getOrCreate()
    //broadcasts the intervals so they can be available to all workers during event pair extraction
    val bintervals = spark.sparkContext.broadcast(intervals)

//    Logger.getLogger("Pair Extraction")
//      .log(Level.INFO, s"Number of input traces ${singleRDD.map(_.id).count()} ")
//    Logger.getLogger("Pair Extraction")
//      .log(Level.INFO, s"Number of unique input traces ${singleRDD.map(_.id).distinct().count()}")

    val full = if (last_checked == null) {
      singleRDD.groupBy(_.id)
        .map(x => {
          this.calculate_pairs_stnm(x._2, null, lookback, bintervals)
        })
    } else {
      singleRDD.groupBy(_.id).leftOuterJoin(last_checked.groupBy(_.id))
        .map(x => {
          val last = x._2._2.orNull
          this.calculate_pairs_stnm(x._2._1, last, lookback, bintervals)
        })
    }

    val pairs = full.flatMap(_._1)
    val last_checked_pairs = full.flatMap(_._2)
//    Logger.getLogger("Pair Extraction").log(Level.INFO, s"Extracted ${pairs.count()} event pairs")
//    Logger.getLogger("Pair Extraction").log(Level.INFO, s"Extracted ${last_checked_pairs.count()} last checked")
    (pairs, last_checked_pairs)

//    if (last_checked == null) {
//      val full = singleRDD.groupBy(_.id)
//        .map(x => {
//          this.calculate_pairs_stnm(x._2, null, lookback, bintervals)
//        })
//      (full.flatMap(_._1),full.flatMap(_._2))
//    } else {
//      val full =singleRDD.groupBy(_.id).leftOuterJoin(last_checked.groupBy(_.id))
//        .map(x => {
//          val last = x._2._2.orNull
//          this.calculate_pairs_stnm(x._2._1, last, lookback, bintervals)
//        })
//      (full.flatMap(_._1),full.flatMap(_._2))
//    }


  }

  /**
   * Extract the event type pairs from the single inverted index
   * @param single The complete single inverted index
   * @param last The list with all the last timestamps that correspond to this event
   * @param lookback The parameter that describes the maximum time difference between two events in a pair
   * @param intervals The list with the intervals
   * @return A tuple, where the first element is the extracted event type pairs and the second element is the last
   *         timestamps for each event type.
   */
  private def calculate_pairs_stnm(single: Iterable[Structs.InvertedSingleFull], last: Iterable[Structs.LastChecked],
                                     lookback: Int, intervals: Broadcast[List[Structs.Interval]]):(List[Structs.PairFull],List[Structs.LastChecked]) = {
    val singleMap: Map[String, Iterable[Structs.InvertedSingleFull]] = single.groupBy(_.event_name)
    val newLastChecked = new ListBuffer[Structs.PairFull]()
    val lastMap = if (last != null) last.groupBy(x => (x.eventA, x.eventB)) else null
    val all_events = single.map(_.event_name).toList.distinct
    val combinations = this.findCombinations(all_events)
    val results: ListBuffer[Structs.PairFull] = new ListBuffer[Structs.PairFull]()
    combinations.foreach(key => { //for each combination
      //extract the timestamp and positions of the event types that described in the combination
      val ts1 = singleMap.getOrElse(key._1, null).map(x => x.times.zip(x.positions)).head
      val ts2 = singleMap.getOrElse(key._2, null).map(x => x.times.zip(x.positions)).head
      val last_checked = try {
        lastMap.getOrElse(key, null).head
      } catch {
        case _: Exception => null
      }
      //detects all the occurrences of this event type pair using the ts1 and ts2
      val nres = this.createTuples(List(key._1, key._2), List(ts1, ts2), intervals, lookback, last_checked,single.head.id)
      //if there are any append them and also keep the last timestamp that they occurred
      if (nres.nonEmpty) {
        newLastChecked += nres.last
        results ++= nres
      }
    })
    val l = newLastChecked.map(x=>{
      Structs.LastChecked(x.eventA,x.eventB,x.id,timestamp = x.timeB.toString)
    })
    //returns the two rdds
    (results.toList, l.toList)

  }

  /**
   * Extract all the occurrences of a particular event type pair by combining the two lists of the occurrences for each
   * event type.
   * @param events A list with two elements, which are the event types
   * @param timestamps A list with two elements, which are the two lists of occurrences for each event type
   * @param bintervals The broadcasted intervals
   * @param lookback The parameter that defines the maximum time difference that two events can have in order to form
   *                 an event type pair
   * @param last_checked The last timestamp that this event type pair occurred in this trace (null if it has not occurred
   *                     yet)
   * @param id The trace id, used to create the required objects
   * @return The list with all the extracted event type pairs
   */
  private def createTuples(events: List[String], timestamps: List[List[(String, Int)]], bintervals: Broadcast[List[Structs.Interval]],
                           lookback: Int, last_checked: Structs.LastChecked, id:Long): List[Structs.PairFull] = {
    var e = Structs.EventWithPosition("", null, -1)
    val oc: ListBuffer[Structs.EventWithPosition] = new ListBuffer[Structs.EventWithPosition]
    var list_id = 0 //begin at first
    //remove all events that have timestamp less than the one in last checked (consider null for first time)
    val nextEvents = this.startAfterLastChecked(timestamps.head, timestamps(1), last_checked)
    while (e != null) {
      val l = timestamps(list_id)
      val x = this.getNextEvent(events(list_id), l, nextEvents(list_id), if(oc.nonEmpty) oc.last.timestamp else null,oc.size)
      nextEvents(list_id) = x._1
      e = x._2
      if (e != null) {
        oc += e
      }
      list_id = (list_id + 1) % 2
    }
    val testSliding = oc.toList.sliding(2,2).toList
    val pairsInit:List[Structs.PairFull] = testSliding.filter(_.size==2).map(x=>{
      val e1 = x.head
      val e2=x(1)
      val interval:Structs.Interval = this.chooseInterval(bintervals,e2.timestamp)
      Structs.PairFull(events.head,events(1),id,e1.timestamp,e2.timestamp,e1.position,e2.position,interval)
    })
      .filter(p=>{
      ChronoUnit.DAYS.between(p.timeA.toInstant,p.timeB.toInstant)<=lookback
    })
    pairsInit
  }


  /**
   * Chooses the appropriate time interval based on the timestamp of the second event in the event type pair
   * @param bintervals The broadcasted time intervals
   * @param ts The timestamp of the second event in the event type pair
   * @return The appropriate interval
   */
  private def chooseInterval(bintervals: Broadcast[List[Structs.Interval]],ts:Timestamp):Structs.Interval={
    for(i<-bintervals.value.indices){
      if(ts.before(bintervals.value(i).end) ){
        return bintervals.value(i)
      }
    }
    null
  }

  /**
   * Used in the createTuples function to get the next viable event from a timestamp list.
   * @param event The event type
   * @param ts The list of the timestamps that this event type occurred in the trace
   * @param start The starting location (in the ts)
   * @param timestamp The timestamp of the last event that was added in the occurrences
   * @param ocs_size The occurrences length. If it is even then the previous event has been completed, whereas if
   *                 it is odd it searches for the second event type to complete the pair.
   * @return A tuple, where the first value is the next position (start+1) and the event (if one is found)
   */
  private def getNextEvent(event: String, ts: List[(String, Int)], start: Int, timestamp: Timestamp, ocs_size: Int): (Int, Structs.EventWithPosition) = {
    var i = start
    if(i>=ts.size){ //there is no such event => terminate
      return (i,null)
    }
    if(timestamp==null){ //there is no previous event => get the next available
      return (start+1,Structs.EventWithPosition(event, Timestamp.valueOf(ts(i)._1), ts(i)._2))
    }
    // ocs_size%2==0 means that the last event type pair has been completed and we are searching for the next first event
    // We differentiate the look for the first or the second event because in the latter version we allow for the same
    // event to appear in two consecutive event type pairs if the pair consists of the same event type pair.
    // For example the trace t=(a,a,a) has two occurrences of (a,a).
    while (i < ts.size ) {
      if (ocs_size%2==1 &&Timestamp.valueOf(ts(i)._1).after(timestamp)) {
        return (i + 1, Structs.EventWithPosition(event, Timestamp.valueOf(ts(i)._1), ts(i)._2))
      } else if(ocs_size%2==0 && !Timestamp.valueOf(ts(i)._1).before(timestamp)){
        return (i + 1, Structs.EventWithPosition(event, Timestamp.valueOf(ts(i)._1), ts(i)._2))
      }
      else {
        i += 1
      }
    }
    (i, null)

  }

  /**
   * Returns a list of two integers. These integers represent the first positions in the ts1 and ts2 respectively,
   * that correspond to events which timestamps are after the last checked timestamp. If the last checked is null,
   * meaning that this is the first time that this event type appears in this trace, it will return (0,0).
   *
   * @param ts1 The timestamp and positions of the first event in the trace
   * @param ts2 The timestamp and positions of the second event in the trace
   * @param lastChecked The last timestmap that this event occurred in this trace
   * @return A list with two integers that represent the first positions in the ts1 and ts2, respectively, that correspond
   *         to events that occur after the lastChecked timestamp.
   */
  private def startAfterLastChecked(ts1: List[(String, Int)], ts2: List[(String, Int)], lastChecked: LastChecked): ListBuffer[Int] = {
    val k = new ListBuffer[Int]
    if (lastChecked == null) {
      k+=0
      k+=0
      k
    }
    else {
      var p1 = 0
      while (p1 < ts1.size && Timestamp.valueOf(ts1(p1)._1).before(Timestamp.valueOf(lastChecked.timestamp))) {
        p1 += 1
      }
      var p2 = 0
      while (p2<ts2.size && Timestamp.valueOf(ts2(p2)._1).before(Timestamp.valueOf(lastChecked.timestamp))) {
        p2 += 1
      }
      k+=p1
      k+=p2
      k
    }
  }

  /**
   * Extracts all the possible event type pairs that can occur in a trace based on the unique event types
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
