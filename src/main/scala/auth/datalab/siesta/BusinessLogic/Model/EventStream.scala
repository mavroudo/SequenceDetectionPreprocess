package auth.datalab.siesta.BusinessLogic.Model

case class EventStream (override val event_type: String,override val timestamp: String, override val position: Int, var trace:String) extends Event(trace,timestamp, event_type,position)
with EventTrait
