package auth.datalab.siesta.BusinessLogic.Metadata

import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.sql.DataFrame

object SetMetadata {

  def initialize_metadata(config: Config): MetaData = {
    MetaData(traces = 0, events = 0, pairs = 0L, lookback = config.lookback_days,
      split_every_days = config.split_every_days, last_interval = "", has_previous_stored = false,
      filename = config.filename, log_name = config.log_name, mode = config.mode, compression = config.compression)
  }

  def load_metadata(metaDataObj:DataFrame):MetaData = {
    metaDataObj.collect().map(x => {
      MetaData(traces = x.getAs("traces"),
        events = x.getAs("events"),
        pairs = x.getAs("pairs"),
        lookback = x.getAs("lookback"), split_every_days = x.getAs("split_every_days"),
        last_interval = x.getAs("last_interval"), has_previous_stored = true,
        filename = x.getAs("filename"), log_name = x.getAs("log_name"), mode = x.getAs("mode"),
        compression = x.getAs("compression"))}).head
  }

}
