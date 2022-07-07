package auth.datalab.sequenceDetection

import auth.datalab.sequenceDetection.CommandLineParser.{Config, ParsingArguments}
import org.apache.log4j.{Level, Logger}



object Main {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: Option[Config] = ParsingArguments.parseArguments(args)
    var config:Config=null
    if(conf.isEmpty){
      System.exit(2)
    }else{
      config = conf.get
    }

    if(config.mode== "siesta"){
      SIESTA.SIESTA.execute(config)
    }else if(config.mode=="signature"){
      Signatures.Signatures.execute(config)
    } else if(config.mode == "setcontainment"){
      SetContainment.SetContainment.execute(config)
    } else{
      println("not a valid choice for mode")
      System.exit(2)
    }
  }

}
