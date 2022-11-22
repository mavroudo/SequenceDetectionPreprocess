package auth.datalab.siesta

import auth.datalab.siesta.CommandLineParser.{Config, ParsingArguments}
import org.apache.log4j.{Level, Logger}


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


    println("Hello World")

//    if (config.mode == "siesta") {
//      SIESTA.SIESTA.execute(config)
//    } else if (config.mode == "positions") {
//      IndexWithPositions.NoTimestamps.execute(config)
//    } else if (config.mode == "object") {
//      ObjectStorage.SIESTA2.execute(config)
//    } else {
//      println("not a valid choice for mode")
//      System.exit(2)
//    }
  }

}
