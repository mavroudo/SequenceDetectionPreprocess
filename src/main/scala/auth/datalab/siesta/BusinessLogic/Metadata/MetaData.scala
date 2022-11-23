package auth.datalab.siesta.BusinessLogic.Metadata

case class MetaData(traces:Int, indexed_tuples:Int, n: Int ,
               lookback: Int, split_every_days:Int,
               last_interval: String, has_previous_stored: Boolean,
              filename:String, log_name: String) extends Serializable {



}
