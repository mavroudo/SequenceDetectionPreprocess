package auth.datalab.siesta.BusinessLogic.Metadata

import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 * This class facilitates the initialization and loading of the Metadata object
 */
object SetMetadata {

  /**
   * Create a new metadata object based on the given configuration object (passed from the [[auth.datalab.siesta.siesta_main]].
   * This method is called if there are no previous records indexed, i.e. it is a new log database
   * @param config The configuration file with the command line parameters
   * @return A new Metadata object
   */
  def initialize_metadata(config: Config): MetaData = {
    MetaData(traces = 0, events = 0, pairs = 0L, lookback = config.lookback_days,has_previous_stored = false,
      filename = config.filename, log_name = config.log_name, mode = config.mode, compression = config.compression,
      last_declare_mined = "")
  }

  /**
   * Loads the previously stored metadata from the database into a Metadata object
   * @param metaDataObj The laoded metadata from the Database
   * @return The metadata object
   */
  def load_metadata(metaDataObj:DataFrame):MetaData = {
    metaDataObj.collect().map(x => {
      val last_declare_mined = Try(x.getAs[String]("last_declare_mined")).getOrElse("")
      MetaData(traces = x.getAs("traces"),
        events = x.getAs("events"),
        pairs = x.getAs("pairs"),
        lookback = x.getAs("lookback"),
        has_previous_stored = true,
        filename = x.getAs("filename"), log_name = x.getAs("log_name"), mode = x.getAs("mode"),
        compression = x.getAs("compression"),
        last_declare_mined = last_declare_mined)}).head
  }

}
