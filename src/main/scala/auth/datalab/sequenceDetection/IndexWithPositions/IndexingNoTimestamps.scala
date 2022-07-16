package auth.datalab.sequenceDetection.IndexWithPositions


import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

object IndexingNoTimestamps {
  case class SequenceWithPositions(events: List[EventWithPositions], sequence_id: Long)

  case class EventWithPositions(position: Long, event: String, timestamp: String)

  case class IdTimeListWithPositions(id: Long, positions: List[Long], times:List[String])

  case class IdTimeListOnlyPositions(id: Long, positions: List[Long])

  case class CassStoreWithPositions(event1: String, event2: String, times: List[IdTimeListOnlyPositions])

  case class EventIdTimeListsEnhanced(event1: String, event2: String, positions: List[IdTimeListOnlyPositions], times:List[Structs.IdTimeList])

  def extract(data: RDD[Structs.Sequence]): RDD[EventIdTimeListsEnhanced] = {
    val spark = SparkSession.builder().getOrCreate()

    val dataTrans: RDD[SequenceWithPositions] = data.map(trace => {
      val events = trace.events.zipWithIndex.map(x => {
        EventWithPositions(x._2, x._1.event,x._1.timestamp)
      })
      SequenceWithPositions(events, trace.sequence_id)
    })

    val combinations = dataTrans
      .flatMap(l => indexWithPositions(l))
      .keyBy(l => (l.event1, l.event2))
      .reduceByKey((a, b) => {
        val newListTimes = List.concat(a.times, b.times)
        val newListPositions = List.concat(a.positions,b.positions)
        EventIdTimeListsEnhanced(a.event1, a.event2, newListPositions,newListTimes)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations

  }

  def indexWithPositions(line: SequenceWithPositions): List[EventIdTimeListsEnhanced] = {
    var mapping = mutable.HashMap[String, (List[Long], List[String])]()
    val sequence_id = line.sequence_id

    line.events.foreach(event => { //mapping events to positions
      val oldSequence = mapping.getOrElse(event.event, null)
      if (oldSequence == null) {
        mapping.+=((event.event, (List(event.position),List(event.timestamp))))
      } else {
        val newListPos = oldSequence._1 :+ event.position
        val newListTimes = oldSequence._2 :+ event.timestamp
        mapping.+=((event.event, (newListPos,newListTimes)))
      }
    })

    mapping.flatMap(eventA => { //double for loop in distinct events
      mapping.map(eventB => {
        val list = List[IdTimeListWithPositions]() :+ this.createPairs(eventA._2, eventB._2, sequence_id)
        val listPos = list.map(x=>{
          IdTimeListOnlyPositions(x.id,x.positions)
        })
        val listTimes = list.map(x=>{
          Structs.IdTimeList(x.id,x.times)
        })
        EventIdTimeListsEnhanced(eventA._1, eventB._1, listPos,listTimes)
      }).filter(x => {
        x.positions.head.positions.nonEmpty
      })
    }).toList

  }

  private def createPairs(eventAtimes: (List[Long],List[String]), eventBtimes: (List[Long],List[String]), sequenceid: Long): IdTimeListWithPositions = {
    case class TempIdList(sequenceid: Long, pos: ListBuffer[(Long,String)])
    var posA = 0
    var posB = 0
    var prev = -1L

    val tA = eventAtimes._1.zip(eventAtimes._2).sortWith((x,y)=>x._1<y._1)
    val tB = eventBtimes._1.zip(eventBtimes._2).sortWith((x,y)=>x._1<y._1)


    val response = TempIdList(sequenceid, new ListBuffer[(Long,String)])
    while (posA < tA.size && posB < tB.size) {
      if (tA(posA)._1 < tB(posB)._1) { // goes in if a  b
        if (prev < tA(posA)._1) {
          response.pos += tA(posA)
          response.pos += tB(posB)
          prev = tB(posB)._1
          posA += 1
          posB += 1
        } else {
          posA += 1
        }
      } else {
        posB += 1
      }
    }
    IdTimeListWithPositions(response.sequenceid, response.pos.map(_._1).toList,response.pos.map(_._2).toList)
  }


}
