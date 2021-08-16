package auth.datalab.sequenceDetection

import auth.datalab.sequenceDetection.SetContainment.SetContainment

object Main {
  def main(args: Array[String]): Unit = {

    if(args(5)=="normal"){
      SequenceDetection.main(args)
    }else if(args(5)=="signature"){
      println("Not yet implemented")
    }else if(args(5)=="setcontainment"){
      SetContainment.main(args)
    }else{
      println("not a valid choice")
    }
  }

}
