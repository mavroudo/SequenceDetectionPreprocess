package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CassandraConnector.ApacheCassandraConnector
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestCountTable extends FunSuite with BeforeAndAfterAll{
//  @transient var dbConnector: DBConnector = new S3Connector()
  @transient var dbConnector: DBConnector = new ApacheCassandraConnector()
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

  test("Write and read Count (2)"){
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
    val counts2 = ExtractCounts.extract(x2._1)
    dbConnector.write_count_table(counts2, metaData)
    val collected =dbConnector.read_count_table(metaData).collect()

    assert(collected.length==14)
    assert(collected.filter(x=> x.id==0 && x.eventA=="a" && x.eventB=="a").head.count==1)
    assert(collected.filter(x=> x.id==0 && x.eventA=="a" && x.eventB=="b").head.count==3)
    assert(collected.filter(x=> x.id==0 && x.eventA=="b" && x.eventB=="a").head.count==2)
    assert(collected.filter(x=> x.id==0 && x.eventA=="b" && x.eventB=="b").head.count==1)

    assert(collected.filter(x=> x.id==1 && x.eventA=="a" && x.eventB=="c").head.count==1)
    assert(collected.filter(x=> x.id==1 && x.eventA=="c" && x.eventB=="a").head.count==1)
    assert(collected.filter(x=> x.id==1 && x.eventA=="a" && x.eventB=="a").head.count==1)


    assert(collected.filter(x => x.id == 2 && x.eventA == "a" && x.eventB == "a").head.count == 1)
    assert(collected.filter(x => x.id == 2 && x.eventA == "a" && x.eventB == "c").head.count == 1)
    assert(collected.filter(x => x.id == 2 && x.eventA == "b" && x.eventB == "c").head.count == 1)
    assert(collected.filter(x => x.id == 2 && x.eventA == "c" && x.eventB == "c").head.count == 1)
    assert(collected.filter(x => x.id == 2 && x.eventA == "c" && x.eventB == "a").head.count == 2)
    assert(collected.filter(x => x.id == 2 && x.eventA == "b" && x.eventB == "a").head.count == 1)
    assert(collected.filter(x => x.id == 2 && x.eventA == "c" && x.eventB == "b").head.count == 1)
  }




  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }


}
