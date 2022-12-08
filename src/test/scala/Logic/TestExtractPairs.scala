package Logic

import ConnectionToDBAndIncrement.CreateRDD
import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3ConnectorTest
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.Timestamp

class TestExtractPairs extends FunSuite with BeforeAndAfterAll{
  @transient var dbConnector: DBConnector = new S3ConnectorTest()
  @transient var metaData: MetaData = null
  @transient var config: Config = null


  override def beforeAll(): Unit = {
    dbConnector.initialize_spark()

  }

  test("Creating pairs - lookback 5d (1)"){
    val spark = SparkSession.builder().getOrCreate()
    config = Config(delete_previous = true, log_name = "test")
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals =  Intervals.intervals(data,"",30)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val x = ExtractPairs.extract(invertedSingleFull,null,intervals,5)
    val pairs = x._1.collect()
    val lastChecked = x._2.collect()
    assert(pairs.length==9)
    assert(lastChecked.length==8)
    assert(Timestamp.valueOf(lastChecked.filter(x=>x.id==0 && x.eventA=="a" && x.eventB=="b").head.timestamp).equals(Timestamp.valueOf("2020-08-20 14:21:02")))
  }

  test("Creating pairs - lookback 2d (1)") {
    val spark = SparkSession.builder().getOrCreate()
    config = Config(delete_previous = true, log_name = "test")
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 2)
    val pairs = x._1.collect()
    val lastChecked = x._2.collect()
    assert(pairs.length == 5)
    assert(lastChecked.length == 4)
    assert(Timestamp.valueOf(lastChecked.filter(x => x.id == 0 && x.eventA == "a" && x.eventB == "b").head.timestamp).equals(Timestamp.valueOf("2020-08-20 14:21:02")))
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }


}
