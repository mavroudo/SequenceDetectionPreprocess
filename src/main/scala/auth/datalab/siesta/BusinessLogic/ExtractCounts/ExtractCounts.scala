package auth.datalab.siesta.BusinessLogic.ExtractCounts

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD

/**
 * This class extracts the statistics for each event type pair. The statistics are:
 *  - total duration: sum of the time difference between every two events that create this event type pair
 *  - total completions: the total number that this event type pair occurred in the database
 *  - minimum duration : the minimum time difference between any two events that create this event type pair
 *  - maximum duration: the maximum time difference between any two events that create this event type pair
 */
object ExtractCounts {


  /**
   * Extracts the statistics for each event type pair
   * @param pairs The RDD that contains all the newly generated event type pairs
   * @return The statistics for each event type pair in the RDD
   */
  def extract(pairs:RDD[Structs.PairFull]):RDD[Structs.Count]={
    pairs.map(x=>{
      val duration = (x.timeB.getTime-x.timeA.getTime)/1000 //store it in seconds
      ((x.eventA,x.eventB),duration,1,duration,duration)
    })
      .keyBy(_._1)
      .reduceByKey((a,b)=>{
        (a._1,a._2+b._2,a._3+b._3,Math.min(a._4,b._4),Math.max(a._5,b._5))
      })
      .map(y=> Structs.Count(y._1._1,y._1._2,y._2._2,y._2._3,y._2._4,y._2._5))
  }
}
