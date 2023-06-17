package auth.datalab.siesta.BusinessLogic.ExtractSingle

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * This class describes how the single inverted index is creating
 */
object ExtractSingle {

  /**
   * Creates the single inverted index based on solely the new events . Additionally this is a previous version
   * when only the timestamps where handled and there was no account for positions of events in the trace.
   *
   * @param invertedSingleFull The newly arrived events
   * @return The RDD containing a single inverted index with List of timestamps
   * @deprecated
   */
  def extract(invertedSingleFull: RDD[Structs.InvertedSingleFull]): RDD[Structs.InvertedSingle] = {
    invertedSingleFull
      .groupBy(_.event_name)
      .map(x => {
        val occurrences = x._2.map(y => Structs.IdTimePositionList(y.id, y.times, y.positions))
        Structs.InvertedSingle(x._1, occurrences.toList)
      })
  }

  /**
   * Creates the single inverted index, using the new events and the last positions per trace
   * @param sequences The newly arrived events
   * @param last_positions The last position of the events stored in the database
   * @return
   */
  def extractFull(sequences: RDD[Structs.Sequence],last_positions:RDD[Structs.LastPosition]): RDD[Structs.InvertedSingleFull] = {
    val rdd = sequences.keyBy(_.sequence_id)
      .join(last_positions.keyBy(_.id))
      .flatMap(x=>{
        val starting = x._2._2.position-x._2._1.events.size
        x._2._1.events.zipWithIndex.map(e=>{
          val event = e._1
          val position = e._2 + starting
          ((event.event, x._1), event.timestamp, position)
        })
      })
      .groupBy(_._1)
      .map(y => {
        val times = y._2.map(_._2)
        val positions = y._2.map(_._3)
        Structs.InvertedSingleFull(y._1._2, y._1._1, times = times.toList, positions = positions.toList)
      })
    rdd
  }

}
