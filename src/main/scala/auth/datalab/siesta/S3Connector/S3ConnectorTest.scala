package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSequence.ExtractSequence
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.net.URI

class S3ConnectorTest extends DBConnector {

  var seq_table: String = _
  var meta_table: String = _
  var single_table: String = _


  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  override def initialize_spark(): Unit = {
    lazy val spark = SparkSession.builder()
      .appName("Object Storage Test")
      .master("local[*]")
      .getOrCreate()

    //TODO: pass through environment vars
    val s3accessKeyAws = "minioadmin"
    val s3secretKeyAws = "minioadmin"
    val connectionTimeOut = "600000"
    val s3endPointLoc: String = "http://rabbit.csd.auth.gr:9000"

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


  }

  /**
   * Create the appropriate tables, remove previous ones
   */
  override def initialize_db(config: Config): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val fs = FileSystem.get(new URI("s3a://siesta/"), spark.sparkContext.hadoopConfiguration)

    //define name tables
    seq_table = s"""s3a://siesta/${config.log_name}/seq/"""
    meta_table = s"""s3a://siesta/${config.log_name}/meta/"""
    single_table = s"""s3a://siesta/${config.log_name}/single/"""

    //delete previous stored values
    if (config.delete_previous) fs.delete(new Path(s"""s3a://siesta/${config.log_name}/"""), true)

    //delete all stored indices in this db
    if (config.delete_all) fs.delete(new Path(s"""s3a://siesta/"""), true)


  }

  /**
   * This method constructs the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   *
   * @param config contains the configuration passed during execution
   * @return the metadata
   */
  override def get_metadata(config: Config): MetaData = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous values if exists
    val schema = ScalaReflection.schemaFor[MetaData].dataType.asInstanceOf[StructType]
    val metaDataObj = try {
      spark.read.schema(schema).json(meta_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
    //calculate new object
    val metaData = if (metaDataObj==null) {
      MetaData(traces = 0, events=0, indexed_tuples = 0, n=config.n, lookback = config.lookback_days,
      split_every_days = config.split_every_days, last_interval = null, has_previous_stored = false,
      filename = config.filename, log_name = config.log_name)
    }else{
      metaDataObj.collect().map(x=>{
        MetaData(traces = x.getAs("traces"),
          events = x.getAs("events"),
          indexed_tuples = x.getAs("indexed_tuples"), n = x.getAs("n"),
          lookback = x.getAs("lookback"), split_every_days = x.getAs("split_every_days"),
          last_interval = x.getAs("last_interval"), has_previous_stored = true,
          filename = x.getAs("filename"), log_name = x.getAs("log_name"))
      }).head
    }



    //persist this version back
    val rdd = spark.sparkContext.parallelize(Seq(metaData))
    val df = rdd.toDF()
    df.write.mode(SaveMode.Overwrite).json(meta_table)

    metaData
  }

  /**
   * Read data as an rdd from the SeqTable
   *
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  override def read_sequence_table(metaData: MetaData): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    try{
      val df = spark.read.parquet(seq_table)
      S3Transformations.transformSeqToRDD(df)
    }catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modify that.
   * If states in the metadata, this method should combine the new traces with the previous ones
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   *
   * @param sequenceRDD RDD containing the traces
   * @param metaData    Containing all the necessary information for the storing
   */
  override def write_sequence_table(sequenceRDD: RDD[Structs.Sequence], metaData: MetaData): RDD[Structs.Sequence] = {
    val previousSequences = this.read_sequence_table(metaData) //get previous
    val combined = this.combine_sequence_table(sequenceRDD,previousSequences) //combine them
    val df = S3Transformations.transformSeqToDF(combined) //write them back
    metaData.traces = df.count()
    df.write.mode(SaveMode.Overwrite).parquet(seq_table)
    combined
  }

  override def combine_sequence_table(newSequences: RDD[Structs.Sequence], previousSequences: RDD[Structs.Sequence]): RDD[Structs.Sequence] = {
    if(previousSequences==null) return newSequences
    previousSequences.keyBy(_.sequence_id)
      .fullOuterJoin(previousSequences.keyBy(_.sequence_id))
      .map(x=>{
        val prevEvents = x._2._1.getOrElse(Structs.Sequence(List(),-1)).events
        val newEvents = x._2._2.getOrElse(Structs.Sequence(List(),-1)).events
        Structs.Sequence(ExtractSequence.combineSequences(prevEvents,newEvents),x._1)
      })
  }


  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and not persist it at the end.
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   *
   * @param singleRDD Contains the single inverted index
   * @param metaData  Containing all the necessary information for the storing
   */
  override def write_single_table(singleRDD: RDD[Structs.InvertedSingleFull], metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    val newEvents = singleRDD.map(x=>x.times.size).reduce((x,y)=>x+y)
    val previousSingle = read_single_table(metaData) //TODO: read only the ones that are required (similar event type)
    val combined = combine_single_table(singleRDD,previousSingle)
    val df = S3Transformations.transformSingleToDF(combined)//transform
    metaData.events+=newEvents//count and update metadata
    df.repartition(col("event_type"))
      .write.partitionBy("event_type")
      .mode(SaveMode.Overwrite).parquet(single_table) //store to s3
    combined
  }

  /**
   * Read data as an rdd from the SingleTable
   *
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  override def read_single_table(metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val df = spark.read.parquet(single_table)
      S3Transformations.transformSingleToRDD(df)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  override def combine_single_table(newSingle: RDD[Structs.InvertedSingleFull], previousSingle: RDD[Structs.InvertedSingleFull]): RDD[Structs.InvertedSingleFull] = {
    if (previousSingle == null) return newSingle
    previousSingle.keyBy(x=>(x.id,x.event_name))
      .fullOuterJoin(newSingle.keyBy(x=>(x.id,x.event_name)))
      .map(x=>{
        val prevTimes = x._2._1.getOrElse(Structs.InvertedSingleFull(-1,"",List())).times
        val newTimes = x._2._2.getOrElse(Structs.InvertedSingleFull(-1,"",List())).times
        Structs.InvertedSingleFull(x._1._1,x._1._2,ExtractSingle.combineTimes(prevTimes,newTimes))
      })
  }
}
