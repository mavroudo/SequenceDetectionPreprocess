package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairsSimple, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.{Sequence, Structs}
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * This class describes the main pipeline of the index building. The procedure includes the following steps:
 *  - Initialize connection with db (configure spark, drop tables if needed, create the new ones)
 *  - Create/Load Metadata object from the configuration object
 *  - Load/Generate traces to be indexed (based on configuration passed)
 *  - Store traces to the SequenceTable
 *  - Extract single index and store it to SingleTable
 *  - Calculate event type pairs and the last timestamp that they occurred per trace (based on the SingleTable data)
 *  - Store inverted index using event type pairs in IndexTable
 *  - Store the last timestamp for each event type per trace in LastChecked table
 *  - Create/Load-merge statistics for each event type pair and store them in CountTable
 *  - Store the updated Metadata object
 *    The methods that are used to perform the following procedures are described in
 *    [[auth.datalab.siesta.BusinessLogic.DBConnector]] so that SIESTA can be connected with a new database by simply
 *    extend this class and implement the corresponding methods.
 */
object SiestaPipeline {

  def execute(c: Config): Unit = {

    // define db connector based on the given param
    val dbConnector = if (c.database == "s3") {
      new S3Connector()
    } else {
      new S3Connector()
    }
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)
    val metadata = dbConnector.get_metadata(c)

    val spark = SparkSession.builder().getOrCreate()
    spark.time({

      val sequenceRDD: RDD[Sequence] = if (!c.duration_determination) {
        IngestingProcess.getData(c)
      } else {
        val detailedSequenceRDD: RDD[Sequence] = IngestingProcess.getDataDetailed(c)
        dbConnector.write_sequence_table(detailedSequenceRDD, metadata, detailed = true)
        detailedSequenceRDD
      }


      //Main pipeline starts here:s
      //      val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c) //load data (either from file or generate)
      //      if (c.duration_determination) {
      //        val detailedSequenceRDD: RDD[Structs.DetailedSequence] = IngestingProcess.getDataDetailed(c)
      //        dbConnector.write_detailed_events_table(detailedSequenceRDD, metadata)
      //      }

      sequenceRDD.persist(StorageLevel.MEMORY_AND_DISK)
      //Extract the last positions of all the traces that are already indexed
      val last_positions: RDD[Structs.LastPosition] = dbConnector.write_sequence_table(sequenceRDD, metadata) //writes traces to sequence table
      last_positions.persist(StorageLevel.MEMORY_AND_DISK)
      //Calculate the intervals based on mix/max timestamp and the last used interval from metadata
      val intervals = Intervals.intervals(sequenceRDD, metadata.last_interval, metadata.split_every_days)
      //Extracts single inverted index (ev_type) -> [(trace_id,ts,pos),...]
      val invertedSingleFull = ExtractSingle.extractFull(sequenceRDD, last_positions)
      sequenceRDD.unpersist()
      last_positions.unpersist()
      //Read and combine the single inverted index with the previous stored
      val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull, metadata)
      //Read last timestamp for each pair for each event
      val lastChecked = dbConnector.read_last_checked_table(metadata)
      //Extract the new pairs and the update lastchecked for each pair for each trace
      //      val x = ExtractPairs.extract(combinedInvertedFull, lastChecked, intervals, metadata.lookback)
      val x = ExtractPairsSimple.extract(combinedInvertedFull, lastChecked, intervals, metadata.lookback)
      combinedInvertedFull.unpersist()

      x._2.persist(StorageLevel.MEMORY_AND_DISK)
      if (dbConnector.isInstanceOf[S3Connector]) {
        Logger.getLogger("LastChecked Table ").log(Level.INFO, s"executing S3")
        val update_last_checked = dbConnector.combine_last_checked_table(x._2, lastChecked)
        //Persist to Index and LastChecked
        dbConnector.write_last_checked_table(update_last_checked, metadata)
      } else {
        Logger.getLogger("LastChecked Table ").log(Level.INFO, s"executing Cassandra")
        dbConnector.write_last_checked_table(x._2, metadata)
      }
      x._2.unpersist()
      x._1.persist(StorageLevel.MEMORY_AND_DISK)
      dbConnector.write_index_table(x._1, metadata, intervals)
      val counts = ExtractCounts.extract(x._1)
      counts.persist(StorageLevel.MEMORY_AND_DISK)
      dbConnector.write_count_table(counts, metadata)
      counts.unpersist()
      x._1.unpersist()
      //Update metadata before exiting
      metadata.has_previous_stored = true
      metadata.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"
      dbConnector.write_metadata(metadata)
      dbConnector.closeSpark()
    })

  }

}
