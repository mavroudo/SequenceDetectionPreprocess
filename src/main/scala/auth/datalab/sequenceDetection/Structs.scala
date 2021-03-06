package auth.datalab.sequenceDetection

import org.apache.spark.sql.types.{DataTypes, StructType}

object Structs {
  //  main dataframe for every event
  val mainDataFrame: StructType = new StructType()
    .add("timestamp", DataTypes.TimestampType, true)
    .add("sequence_id", DataTypes.IntegerType, false)
    .add("event", DataTypes.StringType, false)

  case class Event(timestamp: String, event: String) extends Serializable

  case class Sequence(events: List[Event], sequence_id: Long) extends Serializable

  //general class for event with list of times
  case class EventIdTimeLists(event1: String, event2: String, times: List[IdTimeList])

  //general class for id without time
  case class IdTimeList(id: Long, times: List[String])

  case class PerSequencePairs(event1: String, event2: String, times: IdTimeList)

  case class CassandraIndex(event1_name: String, event2_name: String, sequences: List[String])

  case class Pair(event1: String, event2: String, var first_event: String, pairs: IdTimeList, count: Int)

  case class JoinTemp(event1: String, event2: String, id: String, times: List[String])

  //for count table
  case class CountList(event1_name: String, times: List[(String, Long, Int)])

  case class CassandraCount(event1_name: String, sequences_per_field: List[String])

}
