package auth.datalab.siesta.BusinessLogic.Model



import java.sql.Timestamp

/**
 * This class contains te model that it is used in all the SIESTA pipeline. These methods are
 * not related to any database, they just describe the main process and the databases need to
 * comply with them, by implementing the corresponding methods.
 */
object Structs {

  //General Model
  case class EventWithPosition(event_name:String,timestamp:Timestamp,position:Int)
  case class IdTime(id:String, time: String)
  case class IdTimePositionList(id: String, times: List[String], positions:List[Int])

//  case class Sequence(events: List[Event], sequence_id: Long) extends Serializable
//  case class Event(timestamp: String, event: String) extends Serializable
//
//  class DetailedEvent(var event_type: String,
//                      var start_timestamp: String = "undefined",
//                      var end_timestamp: String,
//                      var waiting_time: Long = 0,
//                      var resource: String,
//                      var trace_id: String) extends Serializable
//  class DetailedSequence(var events: List[DetailedEvent],
//                         var sequence_id: Long) extends Serializable

  //For the single inverted table
  case class InvertedSingle(event_name: String, times: List[IdTimePositionList])
  case class InvertedSingleFull(id: String, event_name: String, times:List[String], positions:List[Int])
  case class LastPosition (id:String, position:Int)

  //For last_checked
  case class LastChecked (eventA:String,eventB:String, id: String, timestamp:String)
  case class LastCheckedDF (eventA:String, eventB:String,occurrences: List[IdTime])
  case class LastCheckedPartitionedDF(eventA:String,eventB:String,timestamp: String, id:String, partition:Long)

  //Extract Pairs
  case class PairFull(eventA:String,eventB:String,id:String,timeA:Timestamp,timeB:Timestamp,positionA:Int,positionB:Int,interval:Interval)
  //Intervals
  case class Interval(start:Timestamp,end:Timestamp) extends Serializable

  //Count
  case class CountList(eventA:String,counts:List[(String,Long,Int,Long,Long,Double)])
  case class Count(eventA:String, eventB:String, sum_duration:Long, count:Int, min_duration:Long, max_duration:Long, sum_squares:Double)

  case class UnsupportedEventTypeException(private val message: String = "",
                                           private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

}
