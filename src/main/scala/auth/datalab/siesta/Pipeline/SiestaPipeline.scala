package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * This class describes the main pipeline of the index building
 */
object SiestaPipeline {

  def execute(c: Config): Unit = {
    
    val dbConnector = new S3Connector()
    dbConnector.initialize_spark()
    val metadata = dbConnector.get_metadata(c)

    //Main pipeline starts here:
    val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c) //load data (either from file or generate)
    sequenceRDD.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_sequence_table(sequenceRDD,metadata) //writes traces to sequence table
    val invertedSingleFull = ExtractSingle.extractFull(sequenceRDD) //calculates inverted single
    invertedSingleFull.persist(StorageLevel.MEMORY_AND_DISK)
    sequenceRDD.unpersist()
    val invertedSingle = ExtractSingle.extract(invertedSingleFull) //writes SingleTable
    dbConnector.write_single_table(invertedSingle,metadata)
    //Up until now there should be no problem with the memory, or time-outs during writing. However creating n-tuples
    //creates large amount of data.










  }

}
