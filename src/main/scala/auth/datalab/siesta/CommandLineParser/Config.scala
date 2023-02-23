package auth.datalab.siesta.CommandLineParser

case class Config(
                   system: String = "siesta",
                   database: String = "s3",
                   mode: String = "positions",
                   filename: String = "synthetic",
                   log_name: String = "synthetic",
                   compression: String = "snappy",
                   delete_all: Boolean = false,
                   delete_previous: Boolean = false,
                   join: Boolean = false,
                   split_every_days: Int = 30,
                   lookback_days: Int = 30,
                   //                   algorithm: String = "indexing",
                   traces: Int = 100,
                   event_types: Int = 10,
                   length_min: Int = 10,
                   length_max: Int = 90,
                   iterations: Int = -1,
                   //                   n: Int = 2, Has been removed after proved to be the most efficient
                   k: Int = -1
                 )
