package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

object ExtractPairs {


  def extract(singleRDD: RDD[Structs.InvertedSingleFull], last_checked: RDD[Structs.LastChecked],
              intervals: List[Structs.Interval], lookback: Int):(RDD[Structs.PairFull],RDD[Structs.LastChecked]) = {

    val spark = SparkSession.builder().getOrCreate()
    val bintervals = spark.sparkContext.broadcast(intervals)
    if (last_checked == null) {
      val full = singleRDD.groupBy(_.id)
        .map(x => {
          this.calculate_pairs_stnm(x._2, null, lookback, bintervals)
        })
      (full.flatMap(_._1),full.flatMap(_._2))
    } else {
      val full =singleRDD.groupBy(_.id).leftOuterJoin(last_checked.groupBy(_.id))
        .map(x => {
          val last = x._2._2.orNull
          this.calculate_pairs_stnm(x._2._1, last, lookback, bintervals)
        })
      (full.flatMap(_._1),full.flatMap(_._2))
    }


  }

  private def calculate_pairs_stnm(single: Iterable[Structs.InvertedSingleFull], last: Iterable[Structs.LastChecked],
                                     lookback: Int, intervals: Broadcast[List[Structs.Interval]]):(List[Structs.PairFull],List[Structs.LastChecked]) = {
    val singleMap: Map[String, Iterable[Structs.InvertedSingleFull]] = single.groupBy(_.event_name)
    val newLastChecked = new ListBuffer[Structs.PairFull]()
    val lastMap = if (last != null) last.groupBy(x => (x.eventA, x.eventB)) else null
    val all_events = single.map(_.event_name).toList.distinct
    val combinations = this.findCombinations(all_events, all_events)
    val results: ListBuffer[Structs.PairFull] = new ListBuffer[Structs.PairFull]()
    combinations.foreach(key => { //for each combination
      val ts1 = singleMap.getOrElse(key._1, null).map(x => x.times.zip(x.positions)).head
      val ts2 = singleMap.getOrElse(key._2, null).map(x => x.times.zip(x.positions)).head
      val last_checked = try {
        lastMap.getOrElse(key, null).head
      } catch {
        case _: Exception => null
      }
      val nres = this.createTuples(List(key._1, key._2), List(ts1, ts2), intervals, lookback, last_checked,single.head.id)
      if (nres.nonEmpty) {
        newLastChecked += nres.last
        results ++= nres
      }
    })
    val l = newLastChecked.map(x=>{
      Structs.LastChecked(x.eventA,x.eventB,x.id,timestamp = x.timeB.toString)
    })
    (results.toList, l.toList)

  }

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


  private def chooseInterval(bintervals: Broadcast[List[Structs.Interval]],ts:Timestamp):Structs.Interval={
    for(i<-bintervals.value.indices){
      if(ts.before(bintervals.value(i).end) ){
        return bintervals.value(i)
      }
    }
    null
  }

  private def getNextEvent(event: String, ts: List[(String, Int)], start: Int, timestamp: Timestamp, ocs_size: Int): (Int, Structs.EventWithPosition) = {
    var i = start
    if(i>=ts.size){ //terminate
      return (i,null)
    }
    if(timestamp==null){
      return (start+1,Structs.EventWithPosition(event, Timestamp.valueOf(ts(i)._1), ts(i)._2))
    }
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


  private def findCombinations(ts1: List[String], ts2: List[String]): List[(String, String)] = {
    ts1.flatMap(t1 => {
      ts2.map(t2 => {
        (t1, t2)
      })
    })
  }


}
