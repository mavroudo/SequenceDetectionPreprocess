package auth.datalab.siesta.BusinessLogic.DBConnector

import auth.datalab.siesta.BusinessLogic.ExtractSequence.ExtractSequence
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.{Event, EventTrait, Sequence, Structs}
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
  def read_sequence_table(metaData: MetaData, detailed: Boolean = false): RDD[EventTrait]


  /**
   * Loads the single inverted index from Cassandra, stored in the SingleTable
   * @param metaData Object containing the metadata
   * @return In RDD the stored data
   */
  def read_single_table(metaData: MetaData): RDD[Event]

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
   * Write count to countTable
   * @param counts Calculated basic statistics per event type pair in order to be stored in the count table
   * @param metaData Object containing the metadata
   */
  def write_count_table(counts:RDD[Structs.Count],metaData: MetaData):Unit

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modified. Additionally, updates metaData object
   *
   * @param sequenceRDD The RDD containing the traces with the new events
   * @param metaData    Object containing the metadata
   */
  def write_sequence_table(sequenceRDD: RDD[EventTrait], metaData: MetaData, detailed: Boolean = false): Unit

  /**
    * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
    * Database should persist it before store it and unpersist it at the end. Additionally, updates metaData object.
    *
    * @param sequenceRDD Contains the newly indexed events in a form of single inverted index
    * @param metaData  Object containing the metadata
    */
  def write_single_table(sequenceRDD: RDD[EventTrait], metaData: MetaData): Unit

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   *
   * @param newPairs  The newly generated pairs
   * @param metaData  Object containing the metadata
   */
  def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData): Unit
}
