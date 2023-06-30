package auth.datalab.siesta.Singatures

import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * This is the class that contains the logic for the Signatures method. It is responsible to build the appropriate
 * indices and store them to Cassandra. The connection to Cassandra is described in [[CassandraConnectionSignatures]]
 */
object Signatures {
  /**
   * Event-type pair
   *
   * @param eventA first event type in the pair
   * @param eventB second event type in the pair
   */
  case class Pair(eventA: String, eventB: String)

  /**
   * Stores the event type pairs extracted from a trace
   *
   * @param sequence_id The trace id
   * @param pairs       The list of the extracted event type pairs
   * @param sequence    The trace
   */
  private case class PairsInSequence(sequence_id: Long, pairs: List[Pair], sequence: Structs.Sequence)

  /**
   * Singature is actually a bit array, here is simple stored as a string. For example, if the frequent event type pairs are
   * [(a,b), (b,d), (a,c), (c,a)] then the signature of the trace t=(a,c,d) will be 10110010. The first 4 digits represent
   * the occurrences of individual events (e.g. a,c and d -> there is no b that is why the second is 0). The last
   * 4 digits represent the occurrence of the pairs (i.e. only (a,c) from the frequent patterns exist and that is why
   * the third digit of the four is 1 and the others are 0)
   *
   * @param signature   The signature of a trace
   * @param sequence_id The trace id
   */
  private case class Signature(signature: String, sequence_id: Long)

  /**
   * Before storing the signature to Cassandra we group the traces that correspond to the same signature, in order
   * to facilitate faster querying latter
   *
   * @param signature    The signature
   * @param sequence_ids The ids of the traces that have the same signature
   */
  case class Signatures(signature: String, sequence_ids: List[String])

  /**
   * This is the main pipeline. It starts by extracting the event type pairs of each trace in order to find the most frequent.
   * If this operation runs in a preexisting index (incremental) then the previously calculated event type pairs are simple
   * retrieved from Cassandra. Then computes the signature for each, group the traces with the same signature and store
   * them in Cassandra.
   *
   * Incremental indexing works (even suboptimal) because the bit array of trace can only change from 0s to 1s as new
   * events are appended in a trace and not the other way around. For an example how the signature is calculated
   * check the case class Signature above.
   *
   * @param c The configuration object passed by the [[auth.datalab.siesta.Main]].
   */
  def execute(c: Config): Unit = {
    val cassandraConnection = new CassandraConnectionSignatures()
    cassandraConnection.startSpark()
    if (c.delete_previous) { //delete previous tables from this log database
      cassandraConnection.dropTables(c.log_name)
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables() // delete all tables in cassandra
    }
    cassandraConnection.createTables(c.log_name) // create the tables for this log database (if not already exist)
    try {
      val spark = SparkSession.builder.getOrCreate()
      //initialize the RDD (either with random generated traces or with traces from a logfile)
      val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c)

      // extract events and frequent pairs and broadcast them to the workers
      val metadata = loadOrCreateSignature(c, sequenceRDD,cassandraConnection)
      val bPairs = spark.sparkContext.broadcast(metadata._2)
      val bEvents = spark.sparkContext.broadcast(metadata._1)
      var time = 0L


      val start = System.currentTimeMillis()
      cassandraConnection.writeTableSeq(sequenceRDD, c.log_name) //write traces
      val combined: RDD[Structs.Sequence] = cassandraConnection.readTableSeq(c.log_name) //read all the traces stored
      val signatures = combined.map(x => createBSignature(x, bEvents, bPairs)) //calculate signatures for all traces
        .groupBy(_.signature) //group traces based on signatures
        .map(x => Signatures(x._1, x._2.map(_.sequence_id.toString).toList))
      signatures.persist(StorageLevel.MEMORY_AND_DISK)
      cassandraConnection.writeTableSign(signatures, c.log_name) //write the signatures back to Cassandra
      sequenceRDD.unpersist()
      signatures.unpersist()
      time = time + System.currentTimeMillis() - start

      println(s"Time taken: $time ms") //prints total execution time
      cassandraConnection.closeSpark() //closes spark connection


    } catch {
      case e: Exception =>
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()


    }
  }


  /**
   * Extract event type pairs a given trace
   *
   * @param line A trace
   * @return The event type pairs that exist in this trace
   */
  private def createPairs(line: Structs.Sequence): PairsInSequence = {
    val l = new mutable.HashSet[Pair]()
    for (i <- line.events.indices) {
      for (j <- i until line.events.size) {
        l += Pair(line.events(i).event, line.events(j).event)
      }
    }
    PairsInSequence(line.sequence_id, l.toList, line)
  }

  /**
   * Generate the [[Signature]] for a given trace, based on the available event types and the most frequent event type
   * pairs.
   *
   * @param sequence      The trace
   * @param events        The available event types
   * @param topKfreqPairs The top-k most frequent event type pairs
   * @return The signature of this trace
   */
  private def createBSignature(sequence: Structs.Sequence, events: Broadcast[List[String]], topKfreqPairs: Broadcast[List[(String, String)]]): Signature = {
    val s = new mutable.StringBuilder()
    val containedEvents = sequence.events.map(_.event).distinct
    for (event <- events.value) {
      if (containedEvents.contains(event)) s += '1' else s += '0'
    }
    val pairs = createPairs(sequence)
    for (freqPair <- topKfreqPairs.value) {
      if (pairs.pairs.map(x => (x.eventA, x.eventB)).contains(freqPair)) s += '1' else s += '0'
    }
    Signature(s.toString(), sequence.sequence_id)
  }

  /**
   * Creates an object that stores the available event types and the frequent event type pairs. This information
   * is either extracted from the Sequence RDD (if the logname is indexed for the first time) or retrieved from
   * Cassandra (if it is an incremental process).
   *
   * @param c                   The configuration object
   * @param sequenceRDD         The RDD that contains all the traces to be indexed
   * @param cassandraConnection Object that handles connection with Cassandra
   *
   * @return A tuple, where the first element is the available event types and the second element is the most frequent
   *         event type pairs.
   */
  private def loadOrCreateSignature(c: Config, sequenceRDD: RDD[Structs.Sequence], cassandraConnection: CassandraConnectionSignatures): (List[String], List[(String, String)]) = {
    val df = cassandraConnection.loadTableMetadata(c.log_name) //retrieves metadata from Cassandra
    if (df.count() == 0) { //first time indexing => extract information
      val events: List[String] = sequenceRDD.flatMap(_.events).map(_.event).distinct().collect().toList
      val k: Int = if (c.k == -1) events.size else c.k
      //extract pairs, count them and keep only the top-k most frequent
      val topKfreqPairs = sequenceRDD.map(createPairs).flatMap(x => x.pairs)
        .map(x => ((x.eventA, x.eventB), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(k)
        .map(_._1)
        .toList
      cassandraConnection.writeTableMetadata(events, topKfreqPairs, c.log_name) //write metadata back to Cassandra
      (events, topKfreqPairs)
    } else { // not the first time indexing => retrieve information from Cassandra
      val pairs: List[(String, String)] = df.filter(row => row.getAs[String]("object") == "pairs")
        .head().getSeq[String](1).toList.map(x => {
        val s = x.split(",")
        (s(0), s(1))
      })
      val events: List[String] = df.filter(row => row.getAs[String]("object") == "events")
        .head().getSeq[String](1).toList
      (events, pairs)
    }

  }

}
