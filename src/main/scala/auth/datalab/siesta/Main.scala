package auth.datalab.siesta

import auth.datalab.siesta.CommandLineParser.{Config, ParsingArguments}
import auth.datalab.siesta.Pipeline.SiestaPipeline
import org.apache.log4j.{Level, Logger}

/**
 * This is the main class. It is responsible to read the command line arguments using the
 * [[ParsingArguments.parseArguments()]] function. Then based on the defined system, pass the configuration object and
 * executes the corresponding pipeline:
 * - "signatures" => [[Singatures.Signatures]]
 * - "set-containment => [[SetContainment.SetContainment]]
 * - "siesta" -> [[SiestaPipeline]] (default)
 */
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
    if(config.system=="signatures"){
      Singatures.Signatures.execute(config)
    }else if(config.system=="set-containment"){
      SetContainment.SetContainment.execute(config)
    }else{
      SiestaPipeline.execute(config)
    }

  }

}
