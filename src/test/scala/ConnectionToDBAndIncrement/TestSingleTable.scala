package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CassandraConnector.ApacheCassandraConnector
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestSingleTable extends FunSuite with BeforeAndAfterAll {
//  @transient var dbConnector:DBConnector = new S3Connector()
  @transient var dbConnector:DBConnector = new ApacheCassandraConnector()
  @transient var metaData:MetaData = null
  @transient var config:Config = null

  override def beforeAll(): Unit = {

    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
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

  test("Write single inverted and read it back (2)") {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    dbConnector.write_single_table(invertedSingleFull, metaData)
    dbConnector.write_sequence_table(data,metaData)

    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val combined = dbConnector.write_sequence_table(data2,metaData)
    val invertedSingleFull2 = ExtractSingle.extractFull(combined)
    val collected = dbConnector.write_single_table(invertedSingleFull2, metaData).collect()
    assert(collected.length==7)
    assert(collected.filter(x=>x.id==0 && x.event_name=="b").head.positions.size==3)
  }

  override def afterAll(): Unit ={
    this.dbConnector.closeSpark()
  }

}
