package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3ConnectorTest
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestSingleTable extends FunSuite with BeforeAndAfterAll {
  @transient var dbConnector:DBConnector = new S3ConnectorTest()
  @transient var metaData:MetaData = null
  @transient var config:Config = null

  override def beforeAll(): Unit = {
    dbConnector.initialize_spark()
    config = Config(delete_previous = true, log_name = "test")
    this.dbConnector.initialize_db(config)
    this.metaData=dbConnector.get_metadata(config)
  }

  test("Calculate single inverted") {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val collected = invertedSingleFull.collect()
    assert(collected.length==7)
    assert(collected.count(_.positions.size != 1)==2)
  }

  test("Write single inverted and read it back (1)") {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val combined = dbConnector.write_single_table(invertedSingleFull,metaData)
    val collected = combined.collect()
    assert(collected.length == 7)
    assert(collected.count(_.positions.size != 1) == 2)
    assert(collected.count(_.positions.size == 1) == 5)
    val stored = dbConnector.read_single_table(metaData)
    val c = stored.collect()
    assert(c.length == 7)
    assert(c.count(_.positions.size != 1) == 2)
    assert(c.count(_.positions.size == 1) == 5)
  }

  override def afterAll(): Unit ={
    this.dbConnector.closeSpark()
  }

}
