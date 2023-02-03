package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CassandraConnector.ApacheCassandraConnector
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class TestSingleTable extends AnyFlatSpec with BeforeAndAfter {
  @transient var dbConnector:DBConnector = new S3Connector()
//  @transient var dbConnector:DBConnector = new ApacheCassandraConnector()
  @transient var metaData:MetaData = null
  @transient var config:Config = null


  before{
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
  }

  it should "Calculate correctly the single inverted index" in {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val lp = dbConnector.write_sequence_table(data,metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data,lp)
    val collected = invertedSingleFull.collect()
    assert(collected.length==7)
    assert(collected.count(_.positions.size != 1)==2)
  }

  it should "Write and read single inverted index (1)" in {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val lp = dbConnector.write_sequence_table(data, metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data, lp)
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
  it should "Write and read single inverted index (2)" in {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val lp = dbConnector.write_sequence_table(data, metaData)
    val invertedSingleFull = ExtractSingle.extractFull(data, lp)
    dbConnector.write_single_table(invertedSingleFull, metaData)

    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val lp2 = dbConnector.write_sequence_table(data2,metaData)
    val invertedSingleFull2 = ExtractSingle.extractFull(data2,lp2)
    val collected = dbConnector.write_single_table(invertedSingleFull2, metaData).collect()
    assert(collected.length==7)
    assert(collected.filter(x=>x.id==0 && x.event_name=="b").head.positions.size==3)
  }


  after{
    this.dbConnector.closeSpark()
  }

}
