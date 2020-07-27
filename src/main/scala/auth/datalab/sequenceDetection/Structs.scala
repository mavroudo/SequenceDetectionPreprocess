package auth.datalab.sequenceDetection
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.deckfour.xes.model.XAttribute

object Structs {
//  main dataframe for every event
  val mainDataFrame: StructType = new StructType()
    .add("timestamp", DataTypes.TimestampType, true)
    .add("sequence_id", DataTypes.IntegerType, false)
    .add("event", DataTypes.StringType, false)

  case class Event(timestamp:String, event:String)

  case class Sequence (events:List[Event], sequence_id:Long)

  //general class for event with list of times
  case class EventIdTimeLists(event1: String, event2: String, times: List[IdTimeList])

  //general class for id without time
  case class IdTimeList(id: Long, val times: List[String])

  case class PerSequencePairs(event1:String,event2:String,times:IdTimeList)

  case class CassandraIndex(event1_name: String, event2_name: String, sequences: List[String])

  case class Pair(event1:String,event2:String,var first_event:String, pairs:IdTimeList, count:Int)
}
