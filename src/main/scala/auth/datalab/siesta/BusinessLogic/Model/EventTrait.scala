package auth.datalab.siesta.BusinessLogic.Model

trait EventTrait extends Serializable {
  def event_type: String
  def timestamp: String
}
