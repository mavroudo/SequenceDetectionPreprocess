import auth.datalab.siesta.BusinessLogic.ExtractPairs.{ExtractPairs, Intervals}
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.{Event, Sequence}
import org.apache.spark.sql.SparkSession

class TestExtractPairs extends FunSuite with BeforeAndAfterAll{
  @transient var sc:SparkContext = null

  def createRDD:RDD[Structs.Sequence]={
    val spark =SparkSession.builder().getOrCreate()
    val events:List[Event]=List(Event("2020-08-15 12:56:42","a"),
      Event("2020-08-15 13:49:53","b"),Event("2020-08-15 14:21:02","a"),
      Event("2020-08-15 14:27:30","c"),Event("2020-08-20 15:27:23","b"))
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

  test("Create single table and extract pairs"){
    val spark=SparkSession.builder().getOrCreate()
    val n =2
    val data = this.createRDD
    val invertedSingleFull = ExtractSingle.extractFull(data)
    val intervals =  Intervals.intervals(data,"",2)

    val all_events = invertedSingleFull.map(_.event_name).distinct().collect()
    val lists = (1 to n).map(_ => all_events).toArray
    val pairs = ExtractPairs.extract(invertedSingleFull,null,intervals,2)
    print("end of test")
  }




}
