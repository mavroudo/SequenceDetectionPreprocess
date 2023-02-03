package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CassandraConnector.ApacheCassandraConnector
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

class TestMetaData extends AnyFlatSpec with BeforeAndAfterAll {
//  @transient var dbConnector = new S3Connector()
  @transient var dbConnector = new ApacheCassandraConnector()

  it should "Get metadata for the first time" in {
    val c = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)
    val metadata = dbConnector.get_metadata(c)
    assert(metadata.last_interval == "")
    assert(!metadata.has_previous_stored)
    assert(metadata.pairs == 0)
    assert(metadata.traces == 0)
    assert(metadata.events == 0)

  }
  it should "Write and read metadata after the first indexing" in {
    val c = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)
    val metadata = dbConnector.get_metadata(c)
    val spark = SparkSession.builder().getOrCreate()

    //Main pipeline starts here:
    val sequenceRDD: RDD[Structs.Sequence] = spark.sparkContext.parallelize(CreateRDD.createRDD_1)

    val combined = dbConnector.write_sequence_table(sequenceRDD, metadata) //writes traces to sequence table and ignore the output
    val intervals = Intervals.intervals(sequenceRDD, metadata.last_interval, metadata.split_every_days)
    metadata.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"

    val invertedSingleFull = ExtractSingle.extractFull(sequenceRDD,combined) //calculates inverted single
    val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull, metadata)
    combinedInvertedFull.persist(StorageLevel.MEMORY_AND_DISK)

    //Up until now there should be no problem with the memory, or time-outs during writing. However creating n-tuples
    //creates large amount of data.
    val lastChecked = dbConnector.read_last_checked_table(metadata)
    val x = ExtractPairs.extract(combinedInvertedFull, lastChecked, intervals, metadata.lookback)
    combinedInvertedFull.unpersist()
    x._2.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_last_checked_table(x._2, metadata)
    x._2.unpersist()
    x._1.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_index_table(x._1, metadata, intervals)
    val counts = ExtractCounts.extract(x._1)
    counts.persist(StorageLevel.MEMORY_AND_DISK)
    dbConnector.write_count_table(counts, metadata)
    counts.unpersist()
    x._1.unpersist()
    metadata.has_previous_stored=true
    dbConnector.write_metadata(metadata)
    val metadata2 = dbConnector.get_metadata(c)
    assert(metadata2.last_interval != "")
    assert(metadata2.has_previous_stored)
    assert(metadata2.pairs == 9)
    assert(metadata2.traces == 3)
    assert(metadata2.events == 9)
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }

}
