package Logic

import ConnectionToDBAndIncrement.CreateRDD
import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestExtractCount extends FunSuite with BeforeAndAfterAll{

  @transient var dbConnector: DBConnector = new S3Connector()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  override def beforeAll(): Unit = {
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
  }

  test("Calculate counts") {
    val spark = SparkSession.builder().getOrCreate()
    config = Config(delete_previous = true, log_name = "test")
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    val counts = ExtractCounts.extract(x._1).collect()
    assert(counts.map(_.count).sum==9)
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }
}
