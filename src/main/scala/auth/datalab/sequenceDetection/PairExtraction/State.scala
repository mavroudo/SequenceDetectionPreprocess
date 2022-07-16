package auth.datalab.sequenceDetection.PairExtraction

import auth.datalab.sequenceDetection.Structs.EventIdTimeLists
import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.HashSet

/**
 * Extracting pairs using State method
 */
object State extends ExtractPairs {


  private class HashMapStructure(distinct_events: List[String], sequence_id: Long) {
    private var index: mutable.HashMap[(String, String), Structs.Pair] = mutable.HashMap()
    distinct_events.foreach(ev1 => { //creating the hashMap
      distinct_events.foreach(ev2 => {
        if (ev1 == ev2) {
          if (index.getOrElse((ev1, ev2), null) == null) index.+=(((ev1, ev2), Structs.Pair(ev1, ev2, null, Structs.IdTimeList(sequence_id, List[String]()), 0)))
        } else {
          index.+=(((ev1, ev2), Structs.Pair(ev1, ev2, null, Structs.IdTimeList(sequence_id, List[String]()), 0)))
        }
      })
    })

    def new_event(event: Structs.Event): Unit = {
      this.check_as_first(event)
      this.check_as_second(event)
    }

    def check_as_first(event: Structs.Event): Unit = {
      this.check_the_same(event)
      distinct_events
        .filter(ev => ev != event.event)
        .foreach(event_b => {
          val pair = index.getOrElse((event.event, event_b), null)
          if (pair != null && pair.count % 2 == 0) {
            val newList = pair.pairs.times :+ event.timestamp
            index.update((event.event, event_b), Structs.Pair(event.event, event_b, event.timestamp, Structs.IdTimeList(pair.pairs.id, newList), pair.count + 1))
          }
        })
    }

    def check_as_second(event: Structs.Event): Unit = {
      distinct_events
        .filter(ev => ev != event.event)
        .foreach(event_a => {
          val pair = index.getOrElse((event_a, event.event), null)
          if (pair != null && pair.count % 2 == 1) {
            val newList = pair.pairs.times :+ event.timestamp
            index.update((event_a, event.event), Structs.Pair(event_a, event.event, pair.first_event, Structs.IdTimeList(pair.pairs.id, newList), pair.count + 1))
          }
        })
    }

    def check_the_same(event: Structs.Event): Unit = {
      val pair: Structs.Pair = index.getOrElse((event.event, event.event), null)
      if (pair != null) {
        val newList = pair.pairs.times :+ event.timestamp
        index.update((event.event, event.event), Structs.Pair(event.event, event.event, event.timestamp, Structs.IdTimeList(pair.pairs.id, newList), pair.count + 1))
      }
    }

    def do_sequence(line: Structs.Sequence): List[Structs.EventIdTimeLists] = {
      line.events.foreach(ev => this.new_event(ev)) //add each event
      val response: List[EventIdTimeLists] = index.map(pair => {
        val newList = List[Structs.IdTimeList]() :+ pair._2.pairs
        Structs.EventIdTimeLists(pair._1._1, pair._1._2, newList)
      }).toList
      response.map(ev => {
        val list = ev.times.map(time => {
          var list = time.times
          if (list.length % 2 != 0) {
            list = list.drop(1)
          }
          Structs.IdTimeList(time.id, list)
        })
        Structs.EventIdTimeLists(ev.event1,ev.event2,list)
      }).filter(ev=>{
        ev.times.head.times.nonEmpty
      })
    }
  }

  override def extract(data: RDD[Structs.Sequence]): RDD[Structs.EventIdTimeLists] = {
    val spark = SparkSession.builder().getOrCreate()
    val combinations = data.flatMap(s => {
      val distinct_events = s.events.map(e => e.event).distinct //first find distinct events
      val structure = new HashMapStructure(distinct_events, s.sequence_id)
      structure.do_sequence(s)
    }).keyBy(l => (l.event1, l.event2)) //combine common combinations of users
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.EventIdTimeLists(a.event1, a.event2, newList)
      }).filter(a => {
      a._2.times.nonEmpty
    })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations
  }

}
