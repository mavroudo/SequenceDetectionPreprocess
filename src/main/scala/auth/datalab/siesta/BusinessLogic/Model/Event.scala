package auth.datalab.siesta.BusinessLogic.Model

class Event(val trace_id:String, val timestamp: String, val event_type: String, val position: Int) extends EventTrait
