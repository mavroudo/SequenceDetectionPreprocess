package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.Metadata.{MetaData, SetMetadata}
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import auth.datalab.siesta.BusinessLogic.Model.{DetailedEvent, Event, EventTrait, Structs}
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.Utils.Utilities
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

class S3ConnectorSimple {

  private var seq_table: String = _
  private var detailed_table: String = _
  private var meta_table: String = _
  private var single_table: String = _
  private var last_checked_table: String = _
  private var index_table: String = _
  private var count_table: String = _

  /**
   * Spark initializes the connection to S3 utilizing the hadoop properties and the aws-bundle library
   */
  def initialize_spark(config: Config): Unit = {
    lazy val spark = SparkSession.builder()
      .appName("SIESTA indexing")
//      .master("local[*]")
      .getOrCreate()

    val s3accessKeyAws = Utilities.readEnvVariable("s3accessKeyAws")
    val s3secretKeyAws = Utilities.readEnvVariable("s3secretKeyAws")
    val s3ConnectionTimeout = Utilities.readEnvVariable("s3ConnectionTimeout")
    val s3endPointLoc: String = Utilities.readEnvVariable("s3endPointLoc")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", config.compression)
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")

    spark.sparkContext.setLogLevel("WARN")
  }

  /**
   * Create the appropriate tables, remove previous ones
   */
  def initialize_db(config: Config): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val fs = FileSystem.get(new URI("s3a://siesta/"), spark.sparkContext.hadoopConfiguration)

    //define name tables
    seq_table = s"""s3a://siesta/${config.log_name}/seq.parquet/"""
    detailed_table = s"""s3a://siesta/${config.log_name}/detailed.parquet/"""
    meta_table = s"""s3a://siesta/${config.log_name}/meta.parquet/"""
    single_table = s"""s3a://siesta/${config.log_name}/single.parquet/"""
    last_checked_table = s"""s3a://siesta/${config.log_name}/last_checked.parquet/"""
    index_table = s"""s3a://siesta/${config.log_name}/index.parquet/"""
    count_table = s"""s3a://siesta/${config.log_name}/count.parquet/"""

    //delete previous stored values
    if (config.delete_previous) {
      fs.delete(new Path(s"""s3a://siesta/${config.log_name}/"""), true)
    }
    //delete all stored indices in this db
    if (config.delete_all) {
      for (l <- fs.listStatus(new Path(s"""s3a://siesta/"""))) {
        fs.delete(l.getPath, true)
      }
    }
  }

  /**
   * This method constructs the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   *
   * @param config contains the configuration passed during execution
   * @return the metadata
   */
  def get_metadata(config: Config): MetaData = {
    Logger.getLogger("Metadata").log(Level.INFO, s"Getting metadata")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()
    //get previous values if exists
    val schema = ScalaReflection.schemaFor[MetaData].dataType.asInstanceOf[StructType]
    val metaDataObj = try {
      spark.read.parquet(meta_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Metadata").log(Level.INFO, s"finished in ${total / 1000} seconds")
    //calculate new metadata object
    val metaData = if (metaDataObj == null) {
      SetMetadata.initialize_metadata(config)
    } else {
      SetMetadata.load_metadata(metaDataObj)
    }
    this.write_metadata(metaData) //persist this version back
    metaData
  }

  /**
   * Persists metadata
   *
   * @param metaData Object containing the metadata
   */
  def write_metadata(metaData: MetaData): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(metaData))
    val df = rdd.toDF()
    df.write.mode(SaveMode.Overwrite).parquet(meta_table)
  }

  /**
   * Read data as an rdd from the SeqTable
   *
   * @param metaData Object containing the metadata
   * @return Object containing the metadata
   */
  def read_sequence_table(metaData: MetaData, detailed: Boolean = false): RDD[EventTrait] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      if (detailed) {
        spark.read.parquet(detailed_table)
          .rdd.map(x => {
            val id = x.getAs[String]("trace_id")
            val event_type = x.getAs[String]("event_type")
            val pos = x.getAs[Int]("position")
            val s_ts = x.getAs[String]("start_timestamp")
            val e_ts = x.getAs[String]("end_timestamp")
            val w_ts = x.getAs[Long]("waiting_time")
            val resource = x.getAs[String]("resource")
            new DetailedEvent(trace_id = id, event_type = event_type, position = pos, start_timestamp = s_ts,
              timestamp = e_ts, waiting_time = w_ts, resource = resource)
          })

      } else {
        spark.read.parquet(seq_table)
          .rdd.map(x => {
            val id = x.getAs[String]("trace_id")
            val event_type = x.getAs[String]("event_type")
            val pos = x.getAs[Int]("position")
            val ts = x.getAs[String]("timestamp")
            new Event(trace_id = id, event_type = event_type, position = pos, timestamp = ts)
          })
      }
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modified. Additionally, updates metaData object
   *
   * @param sequenceRDD The RDD containing the traces with the new events
   * @param metaData    Object containing the metadata
   */
  def write_sequence_table(sequenceRDD: RDD[EventTrait], metaData: MetaData, detailed: Boolean = false): Unit = {
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"Start writing sequence table")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val start = System.currentTimeMillis()
    if (detailed) {
      val df: DataFrame = sequenceRDD.filter(_.isInstanceOf[DetailedEvent]).map(_.asInstanceOf[DetailedEvent])
        .map(y => (y.event_type, y.start_timestamp, y.timestamp, y.waiting_time, y.resource))
        .toDF("trace_id", "event_type", "position", "start_timestamp", "end_timestamp", "waiting_time", "resource")
      df.write.mode(SaveMode.Append).parquet(detailed_table)
    }

    else {
      val df = sequenceRDD
        .map(x => (x.trace_id, x.event_type, x.position, x.timestamp))
        .toDF("trace_id", "event_type", "position", "timestamp")
      metaData.traces += df.filter(_.getAs[Int]("position") == 0).count() //add only new traces in this batch
      metaData.events += df.count()
      df.write.mode(SaveMode.Append).parquet(seq_table)
    }
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and unpersist it at the end. Additionally, updates metaData object.
   *
   * @param singleRDD Contains the newly indexed events in a form of single inverted index
   * @param metaData  Object containing the metadata
   */
  def write_single_table(sequenceRDD: RDD[EventTrait], metaData: MetaData): Unit = {
    Logger.getLogger("Single Table Write").log(Level.INFO, s"Start writing single table")
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val start = System.currentTimeMillis()
    sequenceRDD.map(x => (x.event_type, x.trace_id, x.position, x.timestamp))
      .toDF("event_type", "trace_id", "position", "timestamp")
      .repartition(col("event_type")) //partition based on the event type
      .write
      .partitionBy("event_type")
      .mode(SaveMode.Append)
      .parquet(single_table) //store to s3

    val total = System.currentTimeMillis() - start
    Logger.getLogger("Single Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Loads the single inverted index from S3, stored in the SingleTable
   *
   * @param metaData Object containing the metadata
   * @return In RDD the stored data
   */
  def read_single_table(metaData: MetaData): RDD[Event] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      spark.read.parquet(single_table)
        .rdd
        .map(row => {
          val id = row.getAs[String]("trace_id")
          val event_type = row.getAs[String]("event_type")
          val pos = row.getAs[Int]("position")
          val ts = row.getAs[String]("timestamp")
          new Event(trace_id = id, event_type = event_type, position = pos, timestamp = ts)
        })

    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Returns data from LastChecked Table
   * Loads data from the LastChecked Table, which contains the  information of the last timestamp per event type pair
   * per trace.
   *
   * @param metaData Object containing the metadata
   * @return An RDD with the last timestamps per event type pair per trace
   */
  def read_last_checked_table(metaData: MetaData): RDD[Structs.LastChecked] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      spark.read.parquet(last_checked_table)
        .rdd.map(x => {
          val eventA = x.getAs[String]("eventA")
          val eventB = x.getAs[String]("eventB")
          val timestamp = x.getAs[String]("timestamp")
          val id = x.getAs[String]("trace_id")
          LastChecked(eventA, eventB, id, timestamp)
        })
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Stores new records for last checked back in the database
   *
   * @param lastChecked Records containing the timestamp of last completion for each event type pair for each trace combined
   *                    with the records that were previously read from the LastChecked Table. that way douplicate
   *                    read is avoided
   * @param metaData    Object containing the metadata
   */
  def write_last_checked_table(lastChecked: RDD[Structs.LastChecked], metaData: MetaData): Unit = {
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"Start writing LastChecked table")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = lastChecked.map(x => {
        val formatter = if (metaData.last_checked_split.equals("day")) {
          DateTimeFormatter.ofPattern("yyyy-MM-dd")
        } else if (metaData.last_checked_split.equals("month")) {
          DateTimeFormatter.ofPattern("yyyy-MM")
        } else {
          DateTimeFormatter.ofPattern("yyyy")
        }
        val ts = Timestamp.valueOf(x.timestamp).toLocalDateTime.toLocalDate
        (x.eventA, x.eventB, x.id, x.timestamp, ts.format(formatter))
      })
      .toDF("eventA", "eventB", "trace_id", "timestamp", "partition")

    df.repartition(col("partition"))
      .write.partitionBy("partition")
      .mode(SaveMode.Overwrite).parquet(last_checked_table)

    val total = System.currentTimeMillis() - start
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   *
   * @param newPairs  The newly generated pairs
   * @param metaData  Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted
   */
  def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData): Unit = {
    Logger.getLogger("Index Table Write").log(Level.INFO, s"Start writing Index table")
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val start = System.currentTimeMillis()
    metaData.pairs += newPairs.count() //update metadata
    val df = if (metaData.mode == "positions") {
      newPairs.map(x => {
          (x.eventA, x.eventB, x.id, x.positionA, x.positionB)
        })
        .toDF("eventA", "eventB", "trace_id", "positionA", "positionB")
    } else {
      newPairs.map(x => {
          (x.eventA, x.eventB, x.id, x.timeA, x.timeB)
        })
        .toDF("eventA", "eventB", "trace_id", "timestampA", "timestampB")
    }

    //partition by the interval (start and end) and the first event of the event type pair
    df.repartition(col("eventA"))
      .write.partitionBy("eventA")
      .mode(SaveMode.Append).parquet(this.index_table)
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Index Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Writes count to countTable
   *
   * @param counts   Calculated basic statistics per event type pair in order to be stored in the count table
   * @param metaData Object containing the metadata
   */
  def write_count_table(counts: RDD[Structs.Count], metaData: MetaData): Unit = {
    Logger.getLogger("Count Table Write").log(Level.INFO, s" writing Count table")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val df = counts.groupBy(_.eventA)
      .map(x => {
        val counts = x._2.map(y => (y.eventB, y.sum_duration, y.count, y.min_duration, y.max_duration, y.sum_squares))
        Structs.CountList(x._1, counts.toList)
      })
      .toDF("eventA", "times")
    //partition by the first event in the event type pair
    df.repartition(col("eventA"))
      .write.partitionBy("eventA")
      .mode(SaveMode.Overwrite)
      .parquet(this.count_table)
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Count Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")

  }


}
