package auth.datalab.siesta.BusinessLogic.Model

class DetailedEvent(override val event_type: String,
                    var start_timestamp: String = "undefined",
                    override val timestamp: String, //timestamp = end_timestamp
                    var waiting_time: Long = 0,
                    var resource: String,
                    var trace_id: String) extends Event(timestamp,event_type) with EventTrait
