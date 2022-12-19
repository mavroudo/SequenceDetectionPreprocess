package auth.datalab.siesta.BusinessLogic.ExtractCounts

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD

object ExtractCounts {


  def extract(pairs:RDD[Structs.PairFull]):RDD[Structs.Count]={
    pairs.map(x=>{
      val duration = x.timeB.getTime-x.timeA.getTime
      ((x.eventA,x.eventB),duration,1,duration,duration)
    })
      .keyBy(_._1)
      .reduceByKey((a,b)=>{
        (a._1,a._2+b._2,a._3+b._3,Math.min(a._4,b._4),Math.max(a._5,b._5))
      })
      .map(y=> Structs.Count(y._1._1,y._1._2,y._2._2,y._2._3,y._2._4,y._2._5))
  }
}
