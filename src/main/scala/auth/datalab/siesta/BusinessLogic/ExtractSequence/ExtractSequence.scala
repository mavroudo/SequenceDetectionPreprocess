package auth.datalab.siesta.BusinessLogic.ExtractSequence

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.Utils.Utilities

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * This class merges two subsequences in a single trace. That is, in the incremental indexing, it combines the events
 * in the new log file with the ones that are already indexed and refer to the same trace.
 */
object ExtractSequence {

  /**
   * Merges two subsequences (list of events) into one, by considering that all the events in the first list occurred
   * before any of the events in second list, and that both lists are ordered in ascending ordering in regards with the
   * events timestamp.
   *
   * Even though it might be restricted, if the above limitations does not hold then the generated event type pairs needs
   * to be recalculated for this trace. The support for out of order events is not yet implemented
   *
   * @param x Already indexed part of the trace
   * @param y Newly arrived events that extend this trace
   * @return The combined sequence of events
   */
  def combineSequences(x: List[Structs.Event], y: List[Structs.Event]): List[Structs.Event] = {
    val z: ListBuffer[Structs.Event] = new ListBuffer[Structs.Event]()
    z ++= x
    z ++= y
    z.toList
  }



}
