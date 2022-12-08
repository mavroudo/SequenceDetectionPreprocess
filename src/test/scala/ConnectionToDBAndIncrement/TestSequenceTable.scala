package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3ConnectorTest
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TestSequenceTable extends FunSuite with BeforeAndAfterAll{
  @transient var dbConnector: DBConnector = new S3ConnectorTest()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  override def beforeAll(): Unit = {

    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
  }

  test("Write and read Sequences (1)") {
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    dbConnector.write_sequence_table(data,metaData)
    val collected = dbConnector.read_sequence_table(metaData).collect()
    assert(collected.length == 3)
    assert(collected.count(_.events.size == 3) == 1)
    assert(collected.count(_.events.size == 2) == 1)
    assert(collected.count(_.events.size == 4) == 1)
  }

  test("Read and write Sequences (2)"){
    val spark = SparkSession.builder().getOrCreate()
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    val seq1 = dbConnector.write_sequence_table(data, metaData)
    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val seq2 = dbConnector.write_sequence_table(data2, metaData).collect()
    assert(seq2.length==3)
    assert(seq2.filter(_.sequence_id==0).head.events.size==6)
    assert(seq2.filter(_.sequence_id==1).head.events.size==3)
    assert(seq2.filter(_.sequence_id==2).head.events.size==5)



  }


  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }
}
