package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3ConnectorTest
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestLastChecked extends FunSuite with BeforeAndAfterAll{

  @transient var dbConnector: DBConnector = new S3ConnectorTest()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  override def beforeAll(): Unit = {
    dbConnector.initialize_spark()
    config = Config(delete_previous = true, log_name = "test")
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


  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }

}
