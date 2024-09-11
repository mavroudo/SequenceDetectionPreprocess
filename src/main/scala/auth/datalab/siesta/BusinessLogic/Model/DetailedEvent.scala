package auth.datalab.siesta.BusinessLogic.Model

class DetailedEvent(override val event_type: String,
                    override val position: Int,
                    var start_timestamp: String = "undefined",
                    override val timestamp: String, //timestamp = end_timestamp
                    var waiting_time: Long = 0,
                    var resource: String,
                    override val trace_id: String) extends Event(trace_id,timestamp,event_type,position) with EventTrait
