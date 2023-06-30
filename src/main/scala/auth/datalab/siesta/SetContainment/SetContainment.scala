package auth.datalab.siesta.SetContainment

import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.Serializable

/**
 * This is the class that contains the logic for the Set-Containment method. It is responsible to
 * build the single inverted index and store it along with the traces in cassandra. The connection to
 * Cassandra is described in [[CassandraConnectionSetContainment]]
 */
object SetContainment {
  /**
   * This class is the single inverted index. It contains the event type and a list of
   * all the trace ids that contain this event type. For example, if we have 2 traces t,,1,,=[a,a,b]
   * and trace t,,2,,=[b,c,c], the single inverted index will be:
   *  - (a) -> [1]
   *  - (b) -> [1,2]
   *  - (c) -> [2]
   * @param event The event type
   * @param ids The list of the traces that contains this event type
   */
  case class SetCInverted(event: String, ids: List[Long])

  /**
   * This is the main pipeline. It starts by creating pairs (event type, trace id),
   * the use distinct so each pair appear only once and finally group the pairs based
   * on the event type and order the trace ids (in ascending order).
   * At the end store the traces and the single inverted index in Cassandra.
   *
   * Incremental indexing works, because Cassandra allows to append new records at the end
   * of lists. However, in some cases, it is required to read and merge the already stored lists
   * with the newly calculated. That is because the trace ids needs to be sorted in order for the
   * pruning mechanism to work efficiently.
   *
   * @param c The configuration object passed by the [[auth.datalab.siesta.Main]].
   */
  def execute(c: Config): Unit = {
    val cassandraConnection = new CassandraConnectionSetContainment()
    cassandraConnection.startSpark()
    if (c.delete_previous) {//delete previous tables from this log database
      cassandraConnection.dropTables(c.log_name)
    }
    if (c.delete_all) { // delete all tables in cassandra
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables(c.log_name )// create the tables for this log database (if not already exist)

    try {
      //initialize the RDD (either with random generated traces or with traces from a logfile)
      val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c)
      val start = System.currentTimeMillis()
      //Calculate single inverted index
      val inverted_index: RDD[SetCInverted] = sequenceRDD.flatMap(x => {
        val id = x.sequence_id
        x.events.map(_.event).distinct.map(y => (y, id))
      })
        .distinct //remove duplicates
        .groupBy(_._1) //group them based on event type
        .map(x => {
          val sorted = x._2.toList.map(_._2).distinct.sortWith((a, b) => a < b)
          SetCInverted(x._1, sorted)
        })
      //write results back in Cassandra
      cassandraConnection.writeTableSeq(sequenceRDD, c.log_name)
      cassandraConnection.writeTableSequenceIndex(inverted_index, c.log_name)
      //Unpersist the RDDs, since they were persisted in CassandraConnectionSetContainment
      inverted_index.unpersist()
      sequenceRDD.unpersist()
      val time = System.currentTimeMillis() - start
      //prints total execution time
      println(s"Time taken: $time ms")
      cassandraConnection.closeSpark()//closes spark connection

    } catch {
      case e: Exception =>
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()


    }
  }
}
