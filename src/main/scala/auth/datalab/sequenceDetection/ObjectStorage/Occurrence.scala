package auth.datalab.sequenceDetection.ObjectStorage


class Occurrence(val eventA:String,val eventB:String,val trace:Long, val pA:Long, val pB:Long, val tA:String, val tB:String) extends Serializable {

  def getTuple:(String,String,Long,Long,Long)={
    (eventA,eventB,trace,pA,pB)
  }

  def getPair:(String,String)={
    (eventA,eventB)
  }


}
