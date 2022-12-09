package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.Timestamp

class TestLastChecked extends FunSuite with BeforeAndAfterAll{

  @transient var dbConnector: DBConnector = new S3Connector()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  override def beforeAll(): Unit = {

    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
  }

  test("Write and read from LastChecked (1)"){
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val intervals = Intervals.intervals(data, "", 30)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    dbConnector.write_last_checked_table(x._2,metaData)
    val collected = dbConnector.read_last_checked_table(metaData).collect()
    assert(collected.length==8)

  }

  test("Write and read from LastChecked (2)"){
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    dbConnector.write_sequence_table(data, metaData)
    val intervals = Intervals.intervals(data, "", config.split_every_days)
    metaData.last_interval = s"${intervals.last.start.toString}_${intervals.last.end.toString}"
    val invertedSingleFull = ExtractSingle.extractFull(data)
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
    val invertedSingleFull2 = ExtractSingle.extractFull(d)
    val intervals2 = Intervals.intervals(data2, metaData.last_interval, config.split_every_days)
    val combinedInvertedFull2 = dbConnector.write_single_table(invertedSingleFull2, metaData)
    val last_checked = dbConnector.read_last_checked_table(metaData)
    val x2 = ExtractPairs.extract(combinedInvertedFull2, last_checked, intervals2, config.lookback_days)
    dbConnector.write_last_checked_table(x2._2,metaData)
    val collected = dbConnector.read_last_checked_table(metaData).collect()

    assert(collected.length==14)
    assert(Timestamp.valueOf(collected.filter(x=>x.id==0 && x.eventA=="a" && x.eventB=="b").head.timestamp).equals(Timestamp.valueOf("2020-09-05 12:56:42")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==0 && x.eventA=="b" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-09-03 12:56:42")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==0 && x.eventA=="b" && x.eventB=="b").head.timestamp).equals(Timestamp.valueOf("2020-08-20 14:21:02")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==0 && x.eventA=="a" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-08-19 12:56:42")))

    assert(Timestamp.valueOf(collected.filter(x=>x.id==1 && x.eventA=="a" && x.eventB=="c").head.timestamp).equals(Timestamp.valueOf("2020-08-16 12:11:54")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==1 && x.eventA=="c" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-08-19 12:11:54")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==1 && x.eventA=="a" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-08-19 12:11:54")))


    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="a" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-09-08 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="c" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-09-08 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="a" && x.eventB=="c").head.timestamp).equals(Timestamp.valueOf("2020-09-07 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="b" && x.eventB=="c").head.timestamp).equals(Timestamp.valueOf("2020-09-07 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="c" && x.eventB=="c").head.timestamp).equals(Timestamp.valueOf("2020-09-07 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="c" && x.eventB=="b").head.timestamp).equals(Timestamp.valueOf("2020-08-16 12:31:04")))
    assert(Timestamp.valueOf(collected.filter(x=>x.id==2 && x.eventA=="b" && x.eventB=="a").head.timestamp).equals(Timestamp.valueOf("2020-08-18 12:31:04")))


  }


  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }

}
