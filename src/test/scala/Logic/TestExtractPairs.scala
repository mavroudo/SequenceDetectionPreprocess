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

import java.sql.Timestamp

class TestExtractPairs extends FunSuite with BeforeAndAfterAll{
  @transient var dbConnector: DBConnector = new S3Connector()
  @transient var metaData: MetaData = null
  @transient var config: Config = null

  test("Creating pairs - lookback 5d (1)"){
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
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
    config = Config(delete_previous = true, log_name = "test")
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
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


  test("split every 10 days - lookback 30 (2)"){
    config = Config(delete_previous = true, log_name = "test",split_every_days = 10)
    dbConnector.initialize_spark(config)
    val spark = SparkSession.builder().getOrCreate()
    this.dbConnector.initialize_db(config)
    this.metaData = dbConnector.get_metadata(config)
    val data = spark.sparkContext.parallelize(CreateRDD.createRDD_1)
    dbConnector.write_sequence_table(data,metaData)
    val intervals = Intervals.intervals(data, "", config.split_every_days)
    metaData.last_interval=s"${intervals.last.start.toString}_${intervals.last.end.toString}"
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val combinedInvertedFull = dbConnector.write_single_table(invertedSingleFull,metaData)
    val x = ExtractPairs.extract(combinedInvertedFull, null, intervals, config.lookback_days)
    dbConnector.write_last_checked_table(x._2,metaData)
    dbConnector.write_index_table(x._1,metaData,intervals)
    val counts = ExtractCounts.extract(x._1)
    dbConnector.write_count_table(counts,metaData)
    dbConnector.write_metadata(metaData) //till here index the first one

    //index the second one
    val data2 = spark.sparkContext.parallelize(CreateRDD.createRDD_2)
    val d = dbConnector.write_sequence_table(data2,metaData)
    val invertedSingleFull2 = ExtractSingle.extractFull(d)
    val intervals2 = Intervals.intervals(data2, metaData.last_interval, config.split_every_days)
    val combinedInvertedFull2 = dbConnector.write_single_table(invertedSingleFull2,metaData)
    val last_checked = dbConnector.read_last_checked_table(metaData)
    val x2 = ExtractPairs.extract(combinedInvertedFull2, last_checked, intervals2, config.lookback_days)
    val pairs_c = x2._1.collect()
    val last_checked_c=x2._2.collect()
    assert(pairs_c.length==9)
    assert(last_checked_c.length==9)
    assert(pairs_c.count(x=>x.eventA=="a" && x.eventB=="a")==2)
    assert(pairs_c.count(x=>x.eventA=="c" && x.eventB=="a")==2)
    assert(pairs_c.count(x=>x.eventA=="b" && x.eventB=="a")==1)
    assert(pairs_c.count(x=>x.eventA=="a" && x.eventB=="b")==1)
    assert(pairs_c.count(x=>x.eventA=="c" && x.eventB=="c")==1)
    assert(pairs_c.count(x=>x.eventA=="b" && x.eventB=="c")==1)
    assert(pairs_c.count(x=>x.eventA=="a" && x.eventB=="c")==1)
  }

  test("split every 30 days - lookback 30 (2)") {
    config = Config(delete_previous = true, log_name = "test", split_every_days = 30)
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
    val pairs_c = x2._1.collect()
    val last_checked_c = x2._2.collect()
    assert(pairs_c.length == 9)
    assert(last_checked_c.length == 9)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "a") == 2)
    assert(pairs_c.count(x => x.eventA == "c" && x.eventB == "a") == 2)
    assert(pairs_c.count(x => x.eventA == "b" && x.eventB == "a") == 1)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "b") == 1)
    assert(pairs_c.count(x => x.eventA == "c" && x.eventB == "c") == 1)
    assert(pairs_c.count(x => x.eventA == "b" && x.eventB == "c") == 1)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "c") == 1)
  }

  test("split every 30 days - lookback 10 (2)") {
    config = Config(delete_previous = true, log_name = "test", lookback_days = 10)
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
    val pairs_c = x2._1.collect()
    val last_checked_c = x2._2.collect()
    assert(pairs_c.length == 4)
    assert(last_checked_c.length == 4)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "a") == 1)
    assert(pairs_c.count(x => x.eventA == "c" && x.eventB == "a") == 2)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "b") == 1)
  }

  test("split every 30 days - lookback 15 (2)") {
    config = Config(delete_previous = true, log_name = "test", lookback_days = 15)
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
    val pairs_c = x2._1.collect()
    val last_checked_c = x2._2.collect()
    assert(pairs_c.length == 5)
    assert(last_checked_c.length == 5)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "a") == 1)
    assert(pairs_c.count(x => x.eventA == "c" && x.eventB == "a") == 2)
    assert(pairs_c.count(x => x.eventA == "b" && x.eventB == "a") == 1)
    assert(pairs_c.count(x => x.eventA == "a" && x.eventB == "b") == 1)
  }

  override def afterAll(): Unit = {
    this.dbConnector.closeSpark()
  }


}
