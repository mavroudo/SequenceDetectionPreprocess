package auth.datalab.sequenceDetection.ObjectStorage

import org.apache.spark.sql.Row

class Occurrence(val eventA:String,val eventB:String,val trace:Long, val positionA:Long, val positionB:Long) extends Serializable {

  def getTuple:(String,String,Long,Long,Long)={
    (eventA,eventB,trace,positionA,positionB)
  }

  def getPair:(String,String)={
    (eventA,eventB)
  }


}
