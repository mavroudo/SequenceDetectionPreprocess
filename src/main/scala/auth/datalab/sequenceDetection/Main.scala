package auth.datalab.sequenceDetection

import auth.datalab.sequenceDetection.Signatures.Signature

object Main {
  def main(args: Array[String]): Unit = {
    if (args(5) == "normal") {
      SequenceDetection.main(args)
    } else if (args(5) == "signature") {
      Signature.main(args)
    } else if (args(5) == "setcontainment") {
      SetContainment.SetContainment.main(args)
    } else {
      println("not a valid choice")
    }
  }

}
