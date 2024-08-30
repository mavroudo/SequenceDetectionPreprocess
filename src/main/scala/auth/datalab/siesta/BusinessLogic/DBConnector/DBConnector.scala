package auth.datalab.siesta.BusinessLogic.DBConnector

import auth.datalab.siesta.BusinessLogic.ExtractSequence.ExtractSequence
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.{EventTrait, Sequence, Structs}
import auth.datalab.siesta.BusinessLogic.Model.Structs.{InvertedSingleFull, LastChecked}
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * This class is the interface that each database should extend. There are 3 types of methods described here:
 *  - the write and reads methods - that should be implemented by each database, as they might have different indexing
 * or structure
 *  - the combine methods - that describe how the data already stored in the database are combined
 * with the newly calculated
 *  - the spark related methods - the initialization, building the appropriate tables and finally the termination.
 *
 *  The  tables that are utilized are:
 *   - IndexTable: contains the inverted index that it is based on event type pairs
 *   - SequenceTable: contains the traces that are indexed
 *   - SingleTable: contains the single inverted index (Similar to Set-Containment) where the key is a single event tyep
 *   - LastChecked: contains the timestamp of the last completion of each event type per trace
 *   - CountTable: contains basic statistics, like max duration and number of completions, for each event type pair
 *   - Metadata: contains the information for each log database, like the compression algorithm, the number of traces and
 *   event type pairs etc.
 */

trait DBConnector {
  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  def initialize_spark(config:Config): Unit

  /**
   * Create the appropriate tables, remove previous ones
   */
  def initialize_db(config: Config):Unit

  /**
   * This method constructs the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   * @param config contains the configuration passed during execution
   * @return Obe
   */
  def get_metadata(config:Config): MetaData

  /**
   * Persists metadata
   * @param metaData Object containing the metadata
   */
  def write_metadata(metaData: MetaData):Unit

  /**
   * Closes spark connection
   */
  def closeSpark():Unit={
    SparkSession.builder().getOrCreate().close()
  }


  /**
   * Read data as an rdd from the SeqTable
   * @param metaData Object containing the metadata
   * @return Object containing the metadata
   */
  def read_sequence_table(metaData: MetaData, detailed:Boolean=false):RDD[Sequence]

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modified.
   * This method should combine the results with previous ones and return them to the main pipeline.
   * Additionally updates metaData object
   * @param sequenceRDD The RDD containing the traces
   * @param metaData Object containing the metadata
   * @return An RDD with the last position of the event stored per trace
   */
  def write_sequence_table(sequenceRDD:RDD[Sequence],metaData: MetaData, detailed:Boolean=false): RDD[Structs.LastPosition]

  /**
   * This method is responsible to combine the newly arrived events with the ones previously stored,
   * in order to support incremental indexing.
   * @param newSequences The new sequences to be indexed
   * @param previousSequences The previous sequences that are already indexed
   * @return A combined rdd with all the traces combined
   */
  def combine_sequence_table(newSequences:RDD[Sequence],previousSequences:RDD[Sequence]):RDD[Sequence]= {
    if (previousSequences == null) return newSequences
    val combined = previousSequences.keyBy(_.sequence_id)
      .fullOuterJoin(newSequences.keyBy(_.sequence_id))
      .map{
        case(id,(prevOpt,newOpt))=>
          val prevEvents = prevOpt.map(_.events).getOrElse(List.empty[EventTrait])
          val newEvents = newOpt.map(_.events).getOrElse(List.empty[EventTrait])
          val combinedEvents = ExtractSequence.combineSequences(prevEvents, newEvents)

          new Sequence(combinedEvents,id)
          }
    combined
  }

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and unpersist it at the end.
   * This method should combine the results with the ones previously stored and return them to the main pipeline.
   * Additionally updates metaData object.
   * @param singleRDD Contains the newly indexed events in a form of single inverted index
   * @param metaData Object containing the metadata
   */
  def write_single_table(singleRDD:RDD[Structs.InvertedSingleFull],metaData: MetaData): RDD[Structs.InvertedSingleFull]

  /**
   * Loads the single inverted index from Cassandra, stored in the SingleTable
   * @param metaData Object containing the metadata
   * @return In RDD the stored data
   */
  def read_single_table(metaData: MetaData):RDD[Structs.InvertedSingleFull]



  /**
   * Combine new and previous entries in the Single table
   * @param newSingle New events in single table
   * @param previousSingle Previous events stored in single table
   * @return An RDD with the combined entries
   */
  def combine_single_table(newSingle: RDD[Structs.InvertedSingleFull], previousSingle: RDD[Structs.InvertedSingleFull]): RDD[Structs.InvertedSingleFull] = {
    if (previousSingle == null) return newSingle
    val combined = previousSingle.keyBy(x => (x.id, x.event_name)).fullOuterJoin(newSingle.keyBy(x => (x.id, x.event_name)))
      .map(x => {
        val previous = x._2._1.getOrElse(Structs.InvertedSingleFull("", "", List(), List()))
        val prevOc = previous.times.zip(previous.positions)
        val newly = x._2._2.getOrElse(Structs.InvertedSingleFull("", "", List(), List()))
        val newOc = newly.times.zip(newly.positions)
        val combine = (prevOc ++ newOc).distinct
        val event = if (previous.event_name == "") newly.event_name else previous.event_name
        Structs.InvertedSingleFull(x._1._1, event, combine.map(_._1), combine.map(_._2))
      })
    combined
  }

  /**
   * Returns data from LastChecked Table only the required partitions
   * Loads data from the LastChecked Table, which contains the  information of the last timestamp per event type pair
   * per trace.
   * @param metaData Object containing the metadata
   * @return An RDD with the last timestamps per event type pair per trace
   */
  def read_last_checked_partitioned_table(metaData: MetaData,partitions:List[Long]):RDD[LastChecked]

  /**
   * Returns data from LastChecked Table
   * Loads data from the LastChecked Table, which contains the  information of the last timestamp per event type pair
   * per trace.
   *
   * @param metaData Object containing the metadata
   * @return An RDD with the last timestamps per event type pair per trace
   */
  def read_last_checked_table(metaData: MetaData): RDD[LastChecked]

  /**
   * Stores new records for last checked back in the database
   *
   * @param lastChecked Records containing the timestamp of last completion for each event type pair for each trace
   * @param metaData  Object containing the metadata
   */
  def write_last_checked_table(lastChecked: RDD[LastChecked], metaData: MetaData):Unit

  /**
   * Combines the newly calculated with the previous stored records of last checked
   * @param newLastChecked new records for the last checked
   * @param previousLastChecked already stored records for the last checked values
   */
  def combine_last_checked_table(newLastChecked:RDD[LastChecked], previousLastChecked:RDD[LastChecked]):RDD[LastChecked]= {
    if (previousLastChecked == null) return newLastChecked
    val combined = previousLastChecked.keyBy(x => (x.id, x.eventA, x.eventB))
      .fullOuterJoin(newLastChecked.keyBy(x => (x.id, x.eventA, x.eventB)))
      .map(x => {
        val prevLC = x._2._1.getOrElse(Structs.LastChecked("", "", "", ""))
        val newLC = x._2._2.getOrElse(Structs.LastChecked("", "", "", ""))
        val time = if (newLC.timestamp == "") prevLC.timestamp else newLC.timestamp
        val events = if (newLC.eventA == "") (prevLC.eventA, prevLC.eventB) else (newLC.eventA, newLC.eventB)
        val id = if (newLC.id == -1) prevLC.id else newLC.id
        Structs.LastChecked(events._1, events._2, id, time)
      })
    combined

  }

  /**
   * Loads data from the IndexTable that correspond to the given intervals. These data will be merged with
   * the newly calculated pairs, before written back to the database.
   *
   * @param metaData Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return Loaded records from IndexTable for the given intervals
   */
  def read_index_table(metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]

  /**
   * Loads all the indexed pairs from the IndexTable. Mainly used for testing reasons.
   * Advice: use the above method.
   * @param metaData Object containing the metadata
   * @return Loaded indexed pairs from IndexTable
   */
  def read_index_table(metaData: MetaData):RDD[Structs.PairFull]

  /**
   * Combine the two RDDs with the event type pairs
   *
   * @param newPairs The newly generated event type pairs
   * @param prevPairs The previously stored event type pairs, during the corresponding intervals
   * @param metaData Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted
   * @return The combined event type pairs
   */
  def combine_index_table(newPairs:RDD[Structs.PairFull],prevPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]= {
    if (prevPairs == null) return newPairs
    newPairs.union(prevPairs)
  }

  /**
   * Stores the combined event type pairs back to the database
   * @param newPairs The newly generated event type pairs
   * @param metaData Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted
   */
  def write_index_table(newPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):Unit

  /**
   * Loads previously stored data in the CountTable
   * @param metaData Object containing the metadata
   * @return The data stored in the count table
   */
  def read_count_table(metaData: MetaData):RDD[Structs.Count]

  /**
   * Write count to countTable
   * @param counts Calculated basic statistics per event type pair in order to be stored in the count table
   * @param metaData Object containing the metadata
   */
  def write_count_table(counts:RDD[Structs.Count],metaData: MetaData):Unit


  /**
   * Combine the newly generated count records with the ones previously stored in the database
   * @param newCounts Newly generated count records
   * @param prevCounts Count records stored in the database
   * @param metaData Object containing the metadata
   * @return The combined count records
   */
  def combine_count_table(newCounts: RDD[Structs.Count], prevCounts: RDD[Structs.Count], metaData: MetaData): RDD[Structs.Count] = {
    if (prevCounts == null) return newCounts
    newCounts.keyBy(x => (x.eventA, x.eventB))
      .fullOuterJoin(prevCounts.keyBy(x => (x.eventA, x.eventB)))
      .map(y => {
        val c1 = y._2._1.getOrElse(Structs.Count(y._1._1, y._1._2, 0, 0, -1, -1, 0))
        val c2 = y._2._2.getOrElse(Structs.Count(y._1._1, y._1._2, 0, 0, -1, -1, 0))
        val m = if(c1.min_duration == -1) c2.min_duration
        else if(c2.min_duration == -1) c1.min_duration
        else Math.min(c1.min_duration,c2.min_duration)
        val ma = if (c1.max_duration == -1) c2.max_duration
        else if (c2.max_duration == -1) c1.max_duration
        else Math.max(c1.max_duration, c2.max_duration)
        Structs.Count(c1.eventA, c1.eventB, c1.sum_duration + c2.max_duration, c1.count + c2.count,
          m, ma, Math.pow(c1.sum_duration + c2.max_duration, 2))
      })
  }

}
