package auth.datalab.siesta

import auth.datalab.siesta.CommandLineParser.{Config, ParsingArguments}
import auth.datalab.siesta.Pipeline.SiestaPipeline
import org.apache.log4j.{Level, Logger}
//import org.apache.hadoop.io.compress.ZStandardCodec

object Main {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: Option[Config] = ParsingArguments.parseArguments(args)
    var config: Config = null
    if (conf.isEmpty) {
      System.exit(2)
    } else {
      config = conf.get
    }
    SiestaPipeline.execute(config)
  }

}
