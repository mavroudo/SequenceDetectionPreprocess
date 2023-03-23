package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CassandraConnector.ApacheCassandraConnector
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * This class describes the main pipeline of the index building
 */
object SiestaPipeline {

  def execute(c: Config): Unit = {

    val dbConnector = if (c.database == "cassandra") {
      new ApacheCassandraConnector()
    } else {
      new S3Connector()
    }
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)
    val metadata = dbConnector.get_metadata(c)

    val spark = SparkSession.builder().getOrCreate()
    spark.time({

      //Main pipeline starts here:
      val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c) //load data (either from file or generate)
      sequenceRDD.persist(StorageLevel.MEMORY_AND_DISK)
      //Extract the last positions of all the traces that were logged in
      val last_positions:RDD[Structs.LastPosition] = dbConnector.write_sequence_table(sequenceRDD, metadata) //writes traces to sequence t
      last_positions.persist(StorageLevel.MEMORY_AND_DISK)
      //Calculate the intervals based on mix/max timestamp and the last used interval from metadata
      val intervals = Intervals.intervals(sequenceRDD, metadata.last_interval, metadata.split_every_days)
      //Extracts single inverted index (ev_type) -> [(trace_id,ts,pos),...]
      val invertedSingleFull = ExtractSingle.extractFull(sequenceRDD,last_positions)
      sequenceRDD.unpersist()
      last_positions.unpersist()
      //Read and combine the single inverted index with the previous stored
      val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull, metadata)
      combinedInvertedFull.persist(StorageLevel.MEMORY_AND_DISK)
      //Read last timestamp for each pair for each event
      val lastChecked = dbConnector.read_last_checked_table(metadata)
      //Extract the new pairs and the update lastchecked for each pair for each trace
      val x = ExtractPairs.extract(combinedInvertedFull, lastChecked, intervals, metadata.lookback)
      combinedInvertedFull.unpersist()
      //Persist to Index and LastChecked
      x._2.persist(StorageLevel.MEMORY_AND_DISK)
      dbConnector.write_last_checked_table(x._2, metadata)
      x._2.unpersist()
      x._1.persist(StorageLevel.MEMORY_AND_DISK)
      dbConnector.write_index_table(x._1, metadata, intervals)
      val counts = ExtractCounts.extract(x._1)
      counts.persist(StorageLevel.MEMORY_AND_DISK)
      println("CountTable: "+counts.count().toString)
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
