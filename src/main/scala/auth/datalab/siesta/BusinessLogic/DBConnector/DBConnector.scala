package auth.datalab.siesta.BusinessLogic.DBConnector

import auth.datalab.siesta.BusinessLogic.ExtractSequence.ExtractSequence
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.{InvertedSingleFull, LastChecked}
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
   * @return the metadata
   */
  def get_metadata(config:Config): MetaData

  /**
   * Persists metadata
   * @param metaData metadata of the execution and the database
   */
  def write_metadata(metaData: MetaData):Unit

  def closeSpark():Unit={
    SparkSession.builder().getOrCreate().close()
  }


  /**
   * Read data as an rdd from the SeqTable
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  def read_sequence_table(metaData: MetaData):RDD[Structs.Sequence]

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modify that.
   * If states in the metadata, this method should combine the new traces with the previous ones
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   * @param sequenceRDD RDD containing the traces
   * @param metaData Containing all the necessary information for the storing
   */
  def write_sequence_table(sequenceRDD:RDD[Structs.Sequence],metaData: MetaData): RDD[Structs.Sequence]

  /**
   * This method is responsible to combine results with the previous stored, in order to support incremental indexing
   * @param newSequences The new sequences to be indexed
   * @param previousSequences The previous sequences that are already indexed
   * @return a combined rdd
   */
  def combine_sequence_table(newSequences:RDD[Structs.Sequence],previousSequences:RDD[Structs.Sequence]):RDD[Structs.Sequence]= {
    if (previousSequences == null) return newSequences
    val combined = previousSequences.keyBy(_.sequence_id)
      .fullOuterJoin(newSequences.keyBy(_.sequence_id))
      .map(x => {
        val prevEvents = x._2._1.getOrElse(Structs.Sequence(List(), -1)).events
        val newEvents = x._2._2.getOrElse(Structs.Sequence(List(), -1)).events
        Structs.Sequence(ExtractSequence.combineSequences(prevEvents, newEvents), x._1)
      })
    combined
  }

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and not persist it at the end.
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   * @param singleRDD Contains the single inverted index
   * @param metaData Containing all the necessary information for the storing
   */
  def write_single_table(singleRDD:RDD[Structs.InvertedSingleFull],metaData: MetaData): RDD[Structs.InvertedSingleFull]

  /**
   * Read data as an rdd from the SingleTable
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  def read_single_table(metaData: MetaData):RDD[Structs.InvertedSingleFull]



  /**
   * Combine new and previous entries in the Single table
   * @param newSingle New events in single table
   * @param previousSingle Previous events stored in single table
   * @return the combined lists
   */
  def combine_single_table(newSingle:RDD[Structs.InvertedSingleFull],previousSingle:RDD[Structs.InvertedSingleFull]):RDD[Structs.InvertedSingleFull]= {
    if (previousSingle == null) return newSingle
    val combined = previousSingle.keyBy(x => (x.id, x.event_name))
      .rightOuterJoin(newSingle.keyBy(x => (x.id, x.event_name)))
      .map(x => {
        val previous = x._2._1.getOrElse(Structs.InvertedSingleFull(-1, "", List(), List()))
        val prevOc = previous.times.zip(previous.positions)
        val newOc = x._2._2.times.zip(x._2._2.positions)
        val combine = ExtractSingle.combineTimes(prevOc, newOc).distinct
        Structs.InvertedSingleFull(x._1._1, x._1._2, combine.map(_._1), combine.map(_._2))
      })
    combined
  }

  /**
   * Returns data from LastChecked Table
   * @param metaData Containing all the necessary information for the storing
   * @return LastChecked records
   */
  def read_last_checked_table(metaData: MetaData):RDD[LastChecked]

  /**
   * Writes new records for last checked back in the database and return the combined records with the
   * @param lastChecked records containing the timestamp of last completion for each different n-tuple
   * @param metaData Containing all the necessary information for the storing
   * @return The combined last checked records
   */
  def write_last_checked_table(lastChecked: RDD[LastChecked], metaData: MetaData):RDD[Structs.LastChecked]

  /**
   * Combines the new with the previous stored last checked
   * @param newLastChecked new records for the last checked
   * @param previousLastChecked already stored records for the last checked values
   */
  def combine_last_checked_table(newLastChecked:RDD[LastChecked], previousLastChecked:RDD[LastChecked]):RDD[LastChecked]= {
    if (previousLastChecked == null) return newLastChecked
    val combined = previousLastChecked.keyBy(x => (x.id, x.eventA, x.eventB))
      .fullOuterJoin(newLastChecked.keyBy(x => (x.id, x.eventA, x.eventB)))
      .map(x => {
        val prevLC = x._2._1.getOrElse(Structs.LastChecked("", "", -1, ""))
        val newLC = x._2._2.getOrElse(Structs.LastChecked("", "", -1, ""))
        val time = if (newLC.timestamp == "") prevLC.timestamp else newLC.timestamp
        val events = if (newLC.eventA == "") (prevLC.eventA, prevLC.eventB) else (newLC.eventA, newLC.eventB)
        val id = if (newLC.id == -1) prevLC.id else newLC.id
        Structs.LastChecked(events._1, events._2, id, time)
      })
    combined

  }

  /**
   * Read data previously stored data that correspond to the intervals, in order to be merged
   * @param metaData Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return combined record of pairs during the interval periods
   */
  def read_index_table(metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]

  /**
   * Reads the all the indexed pairs (mainly for testing reasons) advice to use the above method
   * @param metaData Containing all the necessary information for the storing
   * @return All the indexed pairs
   */
  def read_index_table(metaData: MetaData):RDD[Structs.PairFull]

  /**
   * Combine the two rdds with the pairs
   *
   * @param newPairs The newly generated tuples
   * @param prevPairs The previously stored tuples, during the corresponding intervals
   * @param metaData Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted
   * @return the combined pairs
   */
  def combine_index_table(newPairs:RDD[Structs.PairFull],prevPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]= {
    if (prevPairs == null) return newPairs
    newPairs.union(prevPairs)
  }

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   * @param newPairs The newly generated pairs
   * @param metaData Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted
   */
  def write_index_table(newPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):Unit

  /**
   * Read previously stored data in the count table
   * @param metaData Containing all the necessary information for the storing
   * @return The count data stored in the count table
   */
  def read_count_table(metaData: MetaData):RDD[Structs.Count]

  /**
   * Write count to countTable
   * @param counts Calculated basic statistics in order to be stored in the count table
   * @param metaData Containing all the necessary information for the storing
   */
  def write_count_table(counts:RDD[Structs.Count],metaData: MetaData):Unit


  /**
   * Combine the newly generated count records with the previous stored in the database
   * @param newCounts Newly generated count records
   * @param prevCounts Count records stored in the databse
   * @param metaData Containing all the necessary information for the storing
   * @return The combined count records
   */
  def combine_count_table(newCounts: RDD[Structs.Count], prevCounts: RDD[Structs.Count], metaData: MetaData): RDD[Structs.Count] = {
    if (prevCounts == null) return newCounts
    newCounts.keyBy(x => (x.eventA, x.eventB))
      .fullOuterJoin(prevCounts.keyBy(x => (x.eventA, x.eventB)))
      .map(y => {
        val c1 = y._2._1.getOrElse(Structs.Count(y._1._1, y._1._2, 0, 0, -1, -1))
        val c2 = y._2._2.getOrElse(Structs.Count(y._1._1, y._1._2, 0, 0, -1, -1))
        val m = if(c1.min_duration == -1) c2.min_duration
        else if(c2.min_duration == -1) c1.min_duration
        else Math.min(c1.min_duration,c2.min_duration)
        val ma = if (c1.max_duration == -1) c2.max_duration
        else if (c2.max_duration == -1) c1.max_duration
        else Math.max(c1.max_duration, c2.max_duration)
        Structs.Count(c1.eventA, c1.eventB, c1.sum_duration + c2.max_duration, c1.count + c2.count,
          m, ma)
      })
  }

}
