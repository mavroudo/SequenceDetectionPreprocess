package auth.datalab.sequenceDetection.ObjectStorage

import org.apache.spark.sql.Row

class Occurrence(val pair:String,val trace:Long, val positionA:Long, val positionB:Long){

  def getTuple:(String,Long,Long,Long)={
    (pair,trace,positionA,positionB)
  }
}
