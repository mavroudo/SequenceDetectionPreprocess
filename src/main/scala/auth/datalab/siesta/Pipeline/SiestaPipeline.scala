package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * This class describes the main pipeline of the index building
 */
object SiestaPipeline {

  def execute(c: Config): Unit = {
    
    val dbConnector = new S3Connector()
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)
    val metadata = dbConnector.get_metadata(c)



    //Main pipeline starts here:
    val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c) //load data (either from file or generate)

    val combined = dbConnector.write_sequence_table(sequenceRDD,metadata) //writes traces to sequence table and ignore the output
    combined.persist(StorageLevel.MEMORY_AND_DISK)
    val intervals =  Intervals.intervals(sequenceRDD,metadata.last_interval,metadata.split_every_days)
    metadata.last_interval=s"${intervals.last.start.toString}_${intervals.last.end.toString}"

    val invertedSingleFull = ExtractSingle.extractFull(combined) //calculates inverted single
    combined.unpersist()
    val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull,metadata)
    combinedInvertedFull.persist(StorageLevel.MEMORY_AND_DISK)

    //Up until now there should be no problem with the memory, or time-outs during writing. However creating n-tuples
    //creates large amount of data.
    val lastChecked = dbConnector.read_last_checked_table(metadata)
    val x = ExtractPairs.extract(combinedInvertedFull,lastChecked,intervals,metadata.lookback)
    combinedInvertedFull.unpersist()
    x._2.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_last_checked_table(x._2,metadata)
    x._2.unpersist()
    x._1.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_index_table(x._1,metadata,intervals)
    val counts = ExtractCounts.extract(x._1)
    counts.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_count_table(counts,metadata)
    counts.unpersist()
    x._1.unpersist()
    metadata.has_previous_stored=true
    dbConnector.write_metadata(metadata)
    println("Done with this shit")









  }

}
