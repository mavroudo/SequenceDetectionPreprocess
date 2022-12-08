package auth.datalab.siesta.BusinessLogic.DBConnector

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
  def initialize_spark(): Unit

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
  def combine_sequence_table(newSequences:RDD[Structs.Sequence],previousSequences:RDD[Structs.Sequence]):RDD[Structs.Sequence]

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
  def combine_single_table(newSingle:RDD[Structs.InvertedSingleFull],previousSingle:RDD[Structs.InvertedSingleFull]):RDD[Structs.InvertedSingleFull]

  /**
   * Returns data from LastChecked Table
   * @param metaData Containing all the necessary information for the storing
   * @return LastChecked records
   */
  def read_last_checked_table(metaData: MetaData):RDD[LastChecked]

  /**
   * Writes new records for last checked back in the database and return the combined records with the
   * @param lastChecked records containing the timestamp of last completion for each different n-tuple
   * @param metaData
   * @return
   */
  def write_last_checked_table(lastChecked: RDD[LastChecked], metaData: MetaData):RDD[Structs.LastChecked]

  /**
   * Combines the new with the previous stored last checked
   * @param newLastChecked new records for the last checked
   * @param previousLastChecked already stored records for the last checked values
   */
  def combine_last_checked_table(newLastChecked:RDD[LastChecked], previousLastChecked:RDD[LastChecked]):RDD[LastChecked]

  /**
   * Read data previously stored data that correspond to the intervals, in order to be merged
   * @param metaData
   * @param intervals
   * @return
   */
  def read_index_table(metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]

  /**
   * Combine the two rdds with the pairs
   * @param newPairs
   * @param prevPairs
   * @param metaData
   * @param intervals
   * @return
   */
  def combine_index_table(newPairs:RDD[Structs.PairFull],prevPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):RDD[Structs.PairFull]

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   * @param combinedPairs
   * @param metaData
   * @param intervals
   */
  def write_index_table(newPairs:RDD[Structs.PairFull],metaData: MetaData, intervals:List[Structs.Interval]):Unit

  /**
   * Read previously stored data from database
   * @param metaData
   * @return
   */
  def read_count_table(metaData: MetaData):RDD[Structs.Count]

  /**
   * Write count to countTable
   * @param counts
   * @param metaData
   */
  def write_count_table(counts:RDD[Structs.Count],metaData: MetaData):Unit


  def combine_count_table(newCounts:RDD[Structs.Count],prevCounts:RDD[Structs.Count],metaData: MetaData):RDD[Structs.Count]={
    if(prevCounts==null) return newCounts
    newCounts.union(prevCounts)
      .map(x=>((x.eventA,x.eventB,x.id),x.count))
      .reduceByKey((x,y)=>x+y)
      .map(y=>Structs.Count(y._1._1,y._1._2,y._1._3,y._2))
  }

}
