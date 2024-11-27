package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class TestIndexTable extends AnyFlatSpec with BeforeAndAfterAll{

  @transient var dbConnector: DBConnector = new S3Connector()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  it should "Write and Read Index table - positions (1)" in {
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val lp = dbConnector.write_sequence_table(data,metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data,last_positions = lp)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    dbConnector.write_index_table(x._1, metaData, intervals)
    val collected = dbConnector.read_index_table(metaData, intervals).collect()
    assert(collected.length == 9)
    assert(collected.count(_.timeA == null) == 9)
    assert(collected.count(_.timeB == null) == 9)
    assert(collected.count(_.positionA != -1) == 9)
    assert(collected.count(_.positionB != -1) == 9)
  }

  it should "Write and Read Index table - timestamps (1)" in {
    config = Config(delete_previous = true, log_name = "test", mode = "timestamps")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val lp = dbConnector.write_sequence_table(data, metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data, last_positions = lp)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    dbConnector.write_index_table(x._1, metaData, intervals)
    val collected = dbConnector.read_index_table(metaData, intervals).collect()
    assert(collected.length == 9)
    assert(collected.count(_.positionA == -1) == 9)
    assert(collected.count(_.positionB == -1) == 9)
    assert(collected.count(_.timeA != null) == 9)
    assert(collected.count(_.timeB != null) == 9)
  }

  it should "Write and read Index Table - positions (2) " in{
    config = Config(delete_previous = true, log_name = "test", split_every_days = 10)
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val lp = dbConnector.write_sequence_table(data, metaData)
    val intervals = Intervals.intervals(data, "", config.split_every_days)
    metaData.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"
    val invertedSingleFull = ExtractSingle.extractFull(data,lp)
    val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull, metaData)
    val x = ExtractPairs.extract(combinedInvertedFull, null, intervals, config.lookback_days)
    dbConnector.write_last_checked_table(x._2, metaData)
    dbConnector.write_index_table(x._1, metaData, intervals)
    val counts = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts, metaData)
    dbConnector.write_metadata(metaData) //till here index the first one

    //index the second one
    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val d = dbConnector.write_sequence_table(data2, metaData)
    val invertedSingleFull2 = ExtractSingle.extractFull(data2,d)
    val intervals2 = Intervals.intervals(data2, metaData.last_interval, config.split_every_days)
    val combinedInvertedFull2 = dbConnector.write_single_table(invertedSingleFull2, metaData)
    val last_checked = dbConnector.read_last_checked_table(metaData)
    val x2 = ExtractPairs.extract(combinedInvertedFull2, last_checked, intervals2, config.lookback_days)
    dbConnector.write_last_checked_table(x2._2, metaData)
    dbConnector.write_index_table(x2._1, metaData, intervals2)
    val counts2 = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts2, metaData)
    dbConnector.write_metadata(metaData)
    val collected = dbConnector.read_index_table(metaData).collect()
    assert(collected.length==18)
    val lastInterval = List(intervals2.last)
    val collected2 = dbConnector.read_index_table(metaData,lastInterval).collect()
    assert(collected2.length==6)
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }

}
