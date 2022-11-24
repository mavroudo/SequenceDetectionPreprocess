package auth.datalab.siesta.BusinessLogic.ExtractSingle

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.rdd.RDD

/**
 * This object describes how the single inverted index is creating
 */
object ExtractSingle {

    def extract(invertedSingleFull:RDD[Structs.InvertedSingleFull]):RDD[Structs.InvertedSingle]={
      invertedSingleFull
        .groupBy(_.event_name)
        .map(x=>{
          val times = x._2.map(y=>Structs.IdTimeList(y.id,y.times))
          Structs.InvertedSingle(x._1,times.toList)
        })
    }

  def extractFull(sequences: RDD[Structs.Sequence]): RDD[Structs.InvertedSingleFull] = {
    sequences.flatMap(x => {
      x.events.map(event => {
        ((event.event, x.sequence_id), event.timestamp)
      })
    })
      .groupBy(_._1)
      .map(y => {
        val times = y._2.map(_._2)
        Structs.InvertedSingleFull(y._1._2, y._1._1, times.toList)
      })
  }

  def combineTimes(x: List[String], y: List[String]):List[String]= {
    (x, y) match {
      case (Nil, Nil) => Nil
      case (_ :: _, Nil) => x
      case (Nil, _ :: _) => y
      case (i :: _, j :: _) =>
        if (Utilities.compareTimes(i, j))
          i :: combineTimes(x.tail, y)
        else
          j :: combineTimes(x, y.tail)
    }
  }
}
