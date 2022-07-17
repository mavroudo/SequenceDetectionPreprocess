package auth.datalab.sequenceDetection.ObjectStorage

import auth.datalab.sequenceDetection.ObjectStorage.Storage.SingleTable
import auth.datalab.sequenceDetection.Structs.{Event, Sequence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TestSingleTable extends FunSuite with BeforeAndAfterAll{
  @transient var sc:SparkContext=null

  def createRDD:RDD[Sequence]={
    val spark =SparkSession.builder().getOrCreate()
    val events:List[Event]=List(Event("2020-08-15 12:56:42","a"),
      Event("2020-08-15 13:49:53","b"),Event("2020-08-15 14:21:02","a"),
      Event("2020-08-15 14:27:30","c"),Event("2020-08-15 15:27:23","b"))
    val events2:List[Event]=List(Event("2020-08-15 12:11:54","a"),
      Event("2020-08-15 12:39:50","b"),Event("2020-08-15 12:45:22","d"))
    val events3:List[Event]=List(Event("2020-08-15 12:31:04","a"),
      Event("2020-08-15 13:12:59","b"),Event("2020-08-15 14:08:49","a"))
    spark.sparkContext.parallelize(List(Sequence(events,0),Sequence(events2,1),Sequence(events3,2)))
  }

  override def beforeAll(): Unit = {

    val spark = SparkSession.builder()
      .appName("Object Storage Test")
      .master("local[*]")
      .getOrCreate()
    //TODO: pass through environment vars
    val s3accessKeyAws = "minioadmin"
    val s3secretKeyAws = "minioadmin"
    val connectionTimeOut = "600000"
    val s3endPointLoc: String = "http://127.0.0.1:9000"

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", connectionTimeOut)
    //    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sc=spark.sparkContext

  }




  test("Read data from a file that doesn't exist"){
    val spark=SparkSession.builder().getOrCreate()
    try {
      val df = spark.read.parquet("s3a://siesta/log-123-123/idx")
    }catch {
      case e: org.apache.spark.sql.AnalysisException =>
        assert(true)
    }
  }


  test("Create Single inverted"){
    val spark=SparkSession.builder().getOrCreate()
    val data = this.createRDD
    val inverted = SingleTable.calculateSingle(newSequences = data)
    assert(inverted.schema.size==2)
    assert(inverted.schema.head.name=="event_type")
    assert(inverted.schema(1).name=="occurrences")
    val occs = inverted.select("occurrences").rdd.flatMap(x=>{
      x.getAs[Seq[Row]]("occurrences").flatMap(y=>{
        y.getAs[Seq[String]](1).map(t=>(y.getLong(0),t))
      })
    }).collect()
    assert(occs.length==11)
  }


  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
