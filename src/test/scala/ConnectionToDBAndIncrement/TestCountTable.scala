package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll}


class TestCountTable extends AnyFlatSpec with BeforeAndAfterAll {
    @transient var dbConnector: DBConnector = new S3Connector()
  @transient var metaData: MetaData = _
  @transient var config: Config = _

  it should "create correctly the index for one log" in {
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)

    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val last_pos = dbConnector.write_sequence_table(data, metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data, last_positions = last_pos)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    val counts = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts, metaData)
    val collected = dbConnector.read_count_table(metaData)
    assert(collected.map(_.count).sum == 9)
  }

  it should "create correctly the index for two logs" in {
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val last_pos = dbConnector.write_sequence_table(data, metaData)
    val intervals = Intervals.intervals(data, "", config.split_every_days)
    metaData.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"
    val invertedSingleFull = ExtractSingle.extractFull(data, last_pos)
    val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull, metaData)
    val x = ExtractPairs.extract(combinedInvertedFull, null, intervals, config.lookback_days)
    dbConnector.write_last_checked_table(x._2, metaData)
    dbConnector.write_index_table(x._1, metaData, intervals)
    val counts = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts, metaData)
    dbConnector.write_metadata(metaData) //till here index the first one
    val c1 = dbConnector.read_count_table(metaData).collect()
    metaData.has_previous_stored = true
    metaData.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"


    //index the second one
    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val d = dbConnector.write_sequence_table(data2, metaData)
    val invertedSingleFull2 = ExtractSingle.extractFull(data2, last_pos)
    val intervals2 = Intervals.intervals(data2, metaData.last_interval, config.split_every_days)
    val combinedInvertedFull2 = dbConnector.write_single_table(invertedSingleFull2, metaData)
    val last_checked = dbConnector.read_last_checked_table(metaData)
    val x2 = ExtractPairs.extract(combinedInvertedFull2, last_checked, intervals2, config.lookback_days)
    val counts2 = ExtractCounts.extract(x2._1)
    dbConnector.write_count_table(counts2, metaData)
    val collected = dbConnector.read_count_table(metaData).collect()
    assert(collected.length == 9)
    assert(collected.filter(x => x.eventA == "a" && x.eventB == "b").head.count == 3)
    assert(collected.filter(x => x.eventA == "a" && x.eventB == "a").head.count == 3)
    assert(collected.filter(x => x.eventA == "a" && x.eventB == "c").head.count == 2)

    assert(collected.filter(x => x.eventA == "c" && x.eventB == "b").head.count == 1)
    assert(collected.filter(x => x.eventA == "c" && x.eventB == "c").head.count == 1)
    assert(collected.filter(x => x.eventA == "c" && x.eventB == "a").head.count == 3)

    assert(collected.filter(x => x.eventA == "b" && x.eventB == "b").head.count == 1)
    assert(collected.filter(x => x.eventA == "b" && x.eventB == "c").head.count == 1)
    assert(collected.filter(x => x.eventA == "b" && x.eventB == "a").head.count == 3)
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }

}