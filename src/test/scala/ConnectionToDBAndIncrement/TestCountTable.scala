package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3ConnectorTest
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestCountTable extends FunSuite with BeforeAndAfterAll{
  @transient var dbConnector: DBConnector = new S3ConnectorTest()
  @transient var metaData: MetaData = null
  @transient var config: Config = null


  test("Write and read Count (1)"){
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()

    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val intervals = Intervals.intervals(data, "", 30)
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val x = ExtractPairs.extract(invertedSingleFull, null, intervals, 10)
    val counts = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts, metaData)
    val collected = dbConnector.read_count_table(metaData)
    assert(collected.map(_.count).sum==9)
  }


  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }


}
