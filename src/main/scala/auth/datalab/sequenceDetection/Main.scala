package auth.datalab.sequenceDetection

import auth.datalab.sequenceDetection.CommandLineParser.{Config, ParsingArguments}
import auth.datalab.sequenceDetection.Signatures.Signature
import auth.datalab.sequenceDetection.Triplets.ExtractTriplets
import org.apache.log4j.{Level, Logger}
import scopt.OParser



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
    }











//    if (args(5) == "normal") {
//      SequenceDetection.main(args)
//    } else if (args(5) == "big_normal") {
//      SequenceDetectionBigData.main(args)
//    } else if (args(5) == "signature") {
//      Signature.main(args)
//    } else if (args(5) == "big_signature") {
//      Signatures.SignatureBigData.main(args)
//    } else if (args(5) == "setcontainment") {
//      SetContainment.SetContainment.main(args)
//    } else if (args(5) == "big_setcontainment") {
//      SetContainment.SetcontainmentBigData.main(args)
//    }else if(args(5)=="triplets"){
//      ExtractTriplets.main(args)
//    } else {
//      println("not a valid choice")
//    }
  }

}
