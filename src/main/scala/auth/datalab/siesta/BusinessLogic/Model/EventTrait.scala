package auth.datalab.siesta.BusinessLogic.Model

trait EventTrait extends Serializable {
  def event_type: String
  def position: Int
  def timestamp: String
  def trace_id: String
}
