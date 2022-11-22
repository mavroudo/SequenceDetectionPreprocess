package auth.datalab.siesta.BusinessLogic.Model

object Structs {

  //General Model
  case class Event(timestamp: String, event: String) extends Serializable
  case class Sequence(events: List[Event], sequence_id: Long) extends Serializable
  case class IdTimeList(id: Long, times: List[String])

  //For the single inverted table
  case class InvertedSingle(event_name: String, times: List[IdTimeList])
  case class InvertedSingleFull(id: Long, event_name: String, times:List[String])

}
