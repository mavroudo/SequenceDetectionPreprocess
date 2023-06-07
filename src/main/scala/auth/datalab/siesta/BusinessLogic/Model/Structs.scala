package auth.datalab.siesta.BusinessLogic.Model



import java.sql.Timestamp
import java.util.Date

object Structs {

  //For streaming
  case class EventStream(trace:Long, event_type:String, timestamp:Timestamp) extends Serializable

  //General Model
  case class Event(timestamp: String, event: String) extends Serializable
  case class EventWithPosition(event_name:String,timestamp:Timestamp,position:Int)
  case class Sequence(events: List[Event], sequence_id: Long) extends Serializable
  case class IdTimeList(id: Long, times: List[String])
  case class IdTime(id:Long, time: String)
  case class IdTimePositionList(id: Long, times: List[String], positions:List[Int])

  //For the single inverted table
  case class InvertedSingle(event_name: String, times: List[IdTimePositionList])
  case class InvertedSingleFull(id: Long, event_name: String, times:List[String], positions:List[Int])
  case class LastPosition (id:Long, position:Int)

  //For last_checked
  case class LastChecked (eventA:String,eventB:String, id: Long, timestamp:String)
  case class LastCheckedDF (eventA:String, eventB:String,occurrences: List[IdTime])

  //Extract Pairs
  case class PairFull(eventA:String,eventB:String,id:Long,timeA:Timestamp,timeB:Timestamp,positionA:Int,positionB:Int,interval:Interval)
  //Intervals
  case class Interval(start:Timestamp,end:Timestamp) extends Serializable

  //Count
  case class CountList(eventA:String,counts:List[(String,Long,Int,Long,Long)])
  case class Count(eventA:String,eventB:String,sum_duration:Long,count:Int,min_duration:Long,max_duration:Long)

}
