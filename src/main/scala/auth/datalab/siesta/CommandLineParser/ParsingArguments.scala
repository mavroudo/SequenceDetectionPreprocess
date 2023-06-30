package auth.datalab.siesta.CommandLineParser

import scopt.{OParser, OParserBuilder}

import scala.language.postfixOps

/**
 * This class is responsible to parse the parameters from the command line and generate the configuration object.
 * This object will be passed in the different pipelines and control their flow.
 *
 * In case of new functionality it should start from here. Run the preprocess with "--help" to get the full list
 * of available parameters and their use.
 *
 * @see [[Config]], which describes the parameters parsed here.
 */
object ParsingArguments {
  val builder: OParserBuilder[Config] = OParser.builder[Config]
  /**
   * This builder contains the logic for parsing, along with some filters that will make sure that the value is in the
   * appropriate format and is one of the available choices. For example, in the database parameter it will check if
   * the value is either s3 or cassandra and if that is not the case it will stop the execution and report the problem.
   */
  private val parser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("preprocess.jar"),
      head("SIESTA preprocess"),
      opt[String]( "system")
        .action((x, c) => c.copy(system = x))
        .valueName("<system>")
        .validate(x => {
          if (x.equals("siesta") || x.equals("signatures") || x.equals("set-containment") ||x.equals("streaming")) {
            success
          } else {
            failure("Supported values for <system> are siesta, signatures, set-containment or streaming")
          }
        })
        .text("System refers to the system that will be used for indexing"),
      opt[String]('d', "database")
        .action((x, c) => c.copy(database = x))
        .valueName("<database>")
        .validate(x => {
          if (x.equals("s3") || x.equals("cassandra")) {
            success
          } else {
            failure("Supported values for <database> are s3 or cassandra")
          }
        })
        .text("Database refers to the database that will be used to store the index"),
      opt[String]('m', "mode")
        .action((x, c) => c.copy(mode = x))
        .valueName("<mode>")
        .validate(x => {
          if (x.equals("positions") || x.equals("timestamps")) {
            success
          } else {
            failure("Value <mode> must be either positions or timestamps")
          }
        })
        .text("Mode will determine if we use the timestamps or positions in indexing"),
      opt[String]('c', "compression")
        .action((x, c) => c.copy(compression = x))
        .valueName("<compression>")
        .validate(x => {
          if (x.equals("snappy") || x.equals("uncompressed")|| x.equals("lz4") || x.equals("zstd") || x.equals("gzip")) {
            success
          } else {
            failure("Value <compression> must be one of the [snappy, lz4, gzip, zstd, uncompressed]")
          }
        })
        .text("Compression determined the algorithm that will be used to compress the indexes"),
      opt[String]('f', "file")
        .action((x, c) => c.copy(filename = x))
        .valueName("<file>")
        .text("If not set will generate artificially data"),
      opt[String]("logname")
        .action((x, c) => c.copy(log_name = x))
        .valueName("<logname>")
        .text("Specify the name of the index to be created. This is used in case of incremental preprocessing"),
      opt[Unit]("delete_all")
        .action((_, c) => c.copy(delete_all = true))
        .text("cleans all tables in the keyspace"),
      opt[Unit]("delete_prev")
        .action((_, c) => c.copy(delete_previous = true))
        .text("removes all the tables generated by a previous execution of this method"),
//      opt[Unit]("join")
//        .action((_, c) => c.copy(join = true))
//        .text("merges the traces with the already indexed ones"),
      opt[Int]("lookback")
        .action((x, c) => c.copy(lookback_days = x))
        .text("How many days will look back for completions (default=30)")
        .validate(x => {
          if (x > 0) success else failure("Value <lookback> has to be a positive number")
        }),
      opt[Int]('s', "split_every_days")
        .valueName("s")
        .action((x, c) => c.copy(split_every_days = x))
        .text("Split the inverted index every s days (default=30)")
        .validate(x => {
          if (x > 0) success else failure("Value <s> has to be a positive number")
        }),
      note(sys.props("line.separator") + "The parameters below are used if the file was not set and data will be randomly generated"),
      opt[Int]('t', "traces")
        .valueName("<#traces>")
        .validate(x => {
          if (x > 0) success else failure("Value <#traces> must be positive")
        })
        .action((x, c) => c.copy(traces = x)),
      opt[Int]('e', "event_types")
        .valueName("<#event_types>")
        .validate(x => {
          if (x > 0) success else failure("Value <#event_types> must be positive")
        })
        .action((x, c) => c.copy(event_types = x)),
      opt[Int]("lmin")
        .valueName("<min length>")
        .action((x, c) => c.copy(length_min = x)),
      opt[Int]("lmax")
        .valueName("<max length>")
        .action((x, c) => c.copy(length_max = x)),
      help("help").text("prints this usage text")
    )
  }

  /**
   * This method utilizes the above builder to parse the parameters and return the Configuration object back in the
   * [[auth.datalab.siesta.Main]] before deciding the appropriate pipeline to be executed.
   * @param args The command line argumends
   * @return The Configuration object that contains all the parameters (after being evaluated)
   */
  def parseArguments(args: Array[String]): Option[Config] = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        Some(config)
      case _ =>
        println("There was an error with the arguments. Use 'help' to display full list of arguments")
        null
    }
  }


}
