package auth.datalab.siesta.BusinessLogic.Model



import org.apache.spark.streaming.StreamingContext

import java.sql.Timestamp

/**
 * This class contains te model that it is used in all the SIESTA pipeline. These methods are
 * not related to any database, they just describe the main process and the databases need to
 * comply with them, by implementing the corresponding methods.
 */
object Structs {

  //For streaming

  case class StreamingPair(eventA: String, eventB: String, id: String, timeA: Timestamp, timeB: Timestamp, positionA: Int, positionB: Int)

  //General Model
  case class EventWithPosition(event_name:String,timestamp:Timestamp,position:Int)
  case class IdTime(id:String, time: String)
  case class IdTimePositionList(id: String, times: List[String], positions:List[Int])

  //For the single inverted table
  case class InvertedSingle(event_name: String, times: List[IdTimePositionList])
  case class InvertedSingleFull(id: String, event_name: String, times:List[String], positions:List[Int])
  case class LastPosition (id:String, position:Int)

  //For last_checked
  case class LastChecked (eventA:String,eventB:String, id: String, timestamp:String)

  //Extract Pairs
  case class PairFull(eventA:String,eventB:String,id:String,timeA:Timestamp,timeB:Timestamp,positionA:Int,positionB:Int)
  //Count
  case class CountList(eventA:String,counts:List[(String,Long,Int,Long,Long,Double)])
  case class Count(eventA:String, eventB:String, sum_duration:Long, count:Int, min_duration:Long, max_duration:Long, sum_squares:Double)
  //Declare case classes
  case class PositionConstraint(rule:String, event_type:String, occurrences:Double)
  //each activity in how many traces it is contained exactly
  case class ActivityExactly(event_type:String, occurrences: Int, contained:Long)
  case class ExistenceConstraint(rule:String, event_type:String, n: Int, occurrences:Double)
  case class PairConstraint(rule:String, eventA:String, eventB:String, occurrences:Double)

  case class UnorderedHelper(eventA:String,eventB:String, ua:Long, ub:Long, pairs:Long,key:String)
}
