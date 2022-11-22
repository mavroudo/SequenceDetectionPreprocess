package auth.datalab.siesta.BusinessLogic.DBConnector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.rdd.RDD

trait DBConnector {
  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  def initialize_spark(): Unit

  /**
   * This method constructs the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   * @param config contains the configuration passed during execution
   * @return the metadata
   */
  def get_metadata(config:Config): MetaData

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modify that
   * @param sequenceRDD RDD containing the traces
   * @param metaData Containing all the necessary information for the storing
   */
  def write_sequence_table(sequenceRDD:RDD[Structs.Sequence],metaData: MetaData): Unit

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * If databases requires to be persisted in order to accelerate the storage time, they are free to do so.
   * @param singleRDD Contains the single inverted index
   * @param metaData Containing all the necessary information for the storing
   */
  def write_single_table(singleRDD:RDD[Structs.InvertedSingle],metaData: MetaData): Unit



}
