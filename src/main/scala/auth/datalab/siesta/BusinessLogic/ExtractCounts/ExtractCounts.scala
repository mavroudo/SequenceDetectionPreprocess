package auth.datalab.siesta.BusinessLogic.ExtractCounts

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD

object ExtractCounts {


  def extract(pairs:RDD[Structs.PairFull]):RDD[Structs.Count]={
    pairs.map(x=>((x.eventA,x.eventB,x.id),1))
      .reduceByKey(_+_)
      .map(y=>Structs.Count(y._1._1,y._1._2,y._1._3,y._2))
  }
}
