package auth.datalab.siesta.BusinessLogic.Metadata

/**
 * This class describes the metadata object. This object is stored in the database and maintains some key statistics for
 * the log database (e.g. the number of traces, the number of event type pairs) as well as other important parameters
 * (e.g. the compression algorithm that will be used, the values for lookback and split_every_days etc)
 * @param traces The number of indexed traces
 * @param events The number of indexed events
 * @param pairs The number of indexed pairs
 * @param lookback The value for the parameter lookback
 * @param has_previous_stored Flag parameter, shows if there are previously indexed traces in this log database
 * @param filename The name of the last log file indexed in this log database
 * @param log_name The name of the log database
 * @param mode Set to timestamps/positions depending on if the timestamps are stored in the IndexTable or just the events
 *             positions
 * @param start_ts The timestamp of the first event
 * @param last_ts The timestamp of the last event
 * @param compression The compression algorithm that will be used while storing the tables in the database.
 */
case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                    lookback: Int, var has_previous_stored: Boolean,
                    filename: String, log_name: String, mode: String, compression: String,
                    var start_ts:String, var last_ts:String,
                   var last_declare_mined:String) extends Serializable {
}
