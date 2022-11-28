package auth.datalab.siesta.BusinessLogic.Model

import java.util.Date

object Structs {

  //General Model
  case class Event(timestamp: String, event: String) extends Serializable
  case class Sequence(events: List[Event], sequence_id: Long) extends Serializable
  case class IdTimeList(id: Long, times: List[String])
  case class IdTime(id:Long, time: String)
  case class IdTimePositionList(id: Long, times: List[String], positions:List[Int])

  //For the single inverted table
  case class InvertedSingle(event_name: String, times: List[IdTimePositionList])
  case class InvertedSingleFull(id: Long, event_name: String, times:List[String], positions:List[Int])

  //For last_checked
  case class LastChecked (eventA:String,events:List[String], id: Long, timestamp:String)
  case class LastCheckedDF (eventA:String, events:List[String],occurrences: List[IdTime])

  //Extract Pairs

  //Intervals
  case class Interval(start:Date,end:Date)

}
