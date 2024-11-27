package auth.datalab.siesta.BusinessLogic.ExtractSequence

import auth.datalab.siesta.BusinessLogic.Model.{Event, EventTrait, Structs}
import auth.datalab.siesta.Utils.Utilities

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * This class merges two subsequences in a single trace. That is, in the incremental indexing, it combines the events
 * in the new log file with the ones that are already indexed and refer to the same trace.
 */
object ExtractSequence {

}
