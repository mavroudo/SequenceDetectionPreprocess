package auth.datalab.siesta.BusinessLogic.ExtractSingle

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * This object describes how the single inverted index is creating
 */
object ExtractSingle {

  def extract(invertedSingleFull: RDD[Structs.InvertedSingleFull]): RDD[Structs.InvertedSingle] = {
    invertedSingleFull
      .groupBy(_.event_name)
      .map(x => {
        val occurrences = x._2.map(y => Structs.IdTimePositionList(y.id, y.times, y.positions))
        Structs.InvertedSingle(x._1, occurrences.toList)
      })
  }

  def extractFull(sequences: RDD[Structs.Sequence]): RDD[Structs.InvertedSingleFull] = {
    sequences.persist(StorageLevel.MEMORY_AND_DISK)
    val rdd =sequences.flatMap(x => {
      x.events.zipWithIndex.map(e => {
        val event = e._1
        val position = e._2
        ((event.event, x.sequence_id), event.timestamp, position)
      })
    })
      .groupBy(_._1)
      .map(y => {
        val times = y._2.map(_._2)
        val positions = y._2.map(_._3)
        Structs.InvertedSingleFull(y._1._2, y._1._1, times = times.toList, positions = positions.toList)
      })
    sequences.unpersist()
    rdd
  }

  def combineTimes2(x: List[(String, Int)], y: List[(String, Int)]): List[(String, Int)] = {
    val z: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]()
    z ++= x
    z ++= y
    z.toList
  }

  def combineTimes(x: List[(String, Int)], y: List[(String, Int)]): List[(String, Int)] = {
    (x, y) match {
      case (Nil, Nil) => Nil
      case (_ :: _, Nil) => x
      case (Nil, _ :: _) => y
      case (i :: _, j :: _) =>
        //        if (Utilities.compareTimes(i, j))
        if (i._2 < j._2)
          i :: combineTimes(x.tail, y)
        else
          j :: combineTimes(x, y.tail)
    }
  }

}
