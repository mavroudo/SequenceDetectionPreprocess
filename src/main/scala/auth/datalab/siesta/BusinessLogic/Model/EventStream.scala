package auth.datalab.siesta.BusinessLogic.Model

case class EventStream (override val event_type: String,override val timestamp: String, var trace:Long) extends Event(timestamp, event_type)
with EventTrait
