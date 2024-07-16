package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.Metadata.{MetaData, SetMetadata}
import auth.datalab.siesta.BusinessLogic.Model.{Sequence, Structs}
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.Utils.Utilities
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.net.URI

/**
 * This class handles the communication between SIESTA and S3. It inherits all the signatures of the methods from
 * [[auth.datalab.siesta.BusinessLogic.DBConnector]] and overrides the reads and writes to match S3 properties.
 * S3 stores data in parquet files. These files allow for objects to be stored and some indexing that enables efficient query.
 * The drawback is that parquet files are immutable and thus in order to append new records to a preexisting parquet
 * file we have to rewrite the whole file.
 *
 */
class S3Connector extends DBConnector {
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
  override def initialize_spark(config: Config): Unit = {
    lazy val spark = SparkSession.builder()
      .appName("SIESTA indexing")
      .master("local[*]")
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

//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.name", "magic")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.magic.enabled", "true")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", config.compression)
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.sparkContext.setLogLevel("WARN")
  }

  /**
   * Create the appropriate tables, remove previous ones
   */
  override def initialize_db(config: Config): Unit = {
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
  override def get_metadata(config: Config): MetaData = {
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
  override def write_metadata(metaData: MetaData): Unit = {
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
  override def read_sequence_table(metaData: MetaData, detailed:Boolean=false): RDD[Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      if(detailed){
        val df = spark.read.parquet(detailed_table)
        S3Transformations.transformDetailedToRDD(df)
      }else {
        val df = spark.read.parquet(seq_table)
        S3Transformations.transformSeqToRDD(df)
      }
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modified.
   * This method should combine the results with previous ones and return them to the main pipeline.
   * Additionally updates metaData object
   *
   * @param sequenceRDD The RDD containing the traces
   * @param metaData    Object containing the metadata
   * @return An RDD with the last position of the event stored per trace
   */
  override def write_sequence_table(sequenceRDD: RDD[Sequence], metaData: MetaData,detailed:Boolean=false): RDD[Structs.LastPosition] = {
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"Start writing sequence table")
    val start = System.currentTimeMillis()
    val previousSequences = this.read_sequence_table(metaData,detailed) //get previous
    val combined = this.combine_sequence_table(sequenceRDD, previousSequences) //combine them
    if(detailed){
      val df = S3Transformations.transformDetailedToDF(combined) //write them back
      metaData.traces = df.count() //update metadata
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(detailed_table)
    }else{
      val df = S3Transformations.transformSeqToDF(combined) //write them back
      metaData.traces = df.count() //update metadata
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(seq_table)
    }
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    combined.map(x => Structs.LastPosition(x.sequence_id, x.events.size))
  }

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and unpersist it at the end.
   * This method should combine the results with the ones previously stored and return them to the main pipeline.
   * Additionally updates metaData object.
   *
   * @param singleRDD Contains the newly indexed events in a form of single inverted index
   * @param metaData  Object containing the metadata
   */
  override def write_single_table(singleRDD: RDD[Structs.InvertedSingleFull], metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    Logger.getLogger("Single Table Write").log(Level.INFO, s"Start writing single table")
    val start = System.currentTimeMillis()
    val newEvents = singleRDD.map(x => x.times.size).reduce((x, y) => x + y)
    val new_traces = singleRDD.map(_.id).distinct().collect().toSet
    val previousSingle = read_single_table(metaData)
    val combined = combine_single_table(singleRDD, previousSingle)
    combined.persist(StorageLevel.MEMORY_AND_DISK)
    val df = S3Transformations.transformSingleToDF(combined) //transform
    metaData.events += newEvents //count and update metadata
    //partition based on the event type
    df
      .repartition(col("event_type"))
      .write
      .partitionBy("event_type")
      .mode(SaveMode.Overwrite).parquet(single_table) //store to s3

    val total = System.currentTimeMillis() - start
    Logger.getLogger("Single Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    combined.filter(x=>new_traces.contains(x.id))
  }

  /**
   * Loads the single inverted index from S3, stored in the SingleTable
   *
   * @param metaData Object containing the metadata
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

  /**
   * Returns data from LastChecked Table
   * Loads data from the LastChecked Table, which contains the  information of the last timestamp per event type pair
   * per trace.
   *
   * @param metaData Object containing the metadata
   * @return An RDD with the last timestamps per event type pair per trace
   */
  override def read_last_checked_table(metaData: MetaData): RDD[Structs.LastChecked] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val df = spark.read.parquet(last_checked_table)
      if (metaData.last_checked_split == 0) {
        S3Transformations.transformLastCheckedToRDD(df)
      } else {
        S3Transformations.transformPartitionLastCheckedToRDD(df)
      }

    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Returns records from LastChecked Table that corresponds to the given partitions
   * @param metaData Object containing the metadata
   * @param partitions Contains the partitions of the required records from LastChecked. If the list is empty all
   *                   the records from LastChecked will be read.
   *  @return An RDD with the last timestamps per event type pair per trace
   */
  override def read_last_checked_partitioned_table(metaData: MetaData, partitions: List[Long]): RDD[Structs.LastChecked] = {
    if(partitions.isEmpty){
      return read_last_checked_table(metaData)
    }
    val spark = SparkSession.builder().getOrCreate()
    try {
      val parqDF = spark.read.parquet(this.last_checked_table) //loads data
      if (metaData.last_checked_split == 0) {
        S3Transformations.transformLastCheckedToRDD(parqDF)
      } else {
        val parkSQL = parqDF.filter(parqDF("partition").isin(partitions:_*))
        S3Transformations.transformPartitionLastCheckedToRDD(parkSQL)
      }
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
  override def write_last_checked_table(lastChecked: RDD[Structs.LastChecked], metaData: MetaData): Unit = {
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"Start writing LastChecked table")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()

    if (metaData.last_checked_split == 0) { //no partition was used
      val df = S3Transformations.transformLastCheckedToDF(lastChecked) //transform them
      df.repartition(col("eventA"))
        .write.partitionBy("eventA")
        .mode(SaveMode.Overwrite).parquet(last_checked_table)
    } else { //transform them using using the partition
      val df = S3Transformations.transformLastCheckedToPartitionedDF(lastChecked, metaData)
      df
        .repartition(col("partition"), col("eventA"))
        .write.partitionBy("partition", "eventA")
        .mode(SaveMode.Overwrite).parquet(last_checked_table)
    }
    val total = System.currentTimeMillis() - start
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Loads data from the IndexTable that correspond to the given intervals. These data will be merged with
   * the newly calculated pairs, before written back to the database.
   *
   * @param metaData  Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return Loaded records from IndexTable for the given intervals
   */
  override def read_index_table(metaData: MetaData, intervals: List[Structs.Interval]): RDD[Structs.PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val parqDF = spark.read.parquet(this.index_table) //loads data
      parqDF.createOrReplaceTempView("IndexTable")
      //select only the required intervals
      val interval_min = intervals.map(_.start).distinct.sortWith((x, y) => x.before(y)).head
      val interval_max = intervals.map(_.end).distinct.sortWith((x, y) => x.before(y)).last
      val parkSQL = spark.sql(s"""select * from IndexTable where (start>=to_timestamp('$interval_min') and end<=to_timestamp('$interval_max'))""")
      S3Transformations.transformIndexToRDD(parkSQL, metaData) //transform the data
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Loads all the indexed pairs from the IndexTable. Mainly used for testing reasons.
   * Advice: use the above method.
   *
   * @param metaData Object containing the metadata
   * @return Loaded indexed pairs from IndexTable
   */
  override def read_index_table(metaData: MetaData): RDD[Structs.PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val parqDF = spark.read.parquet(this.index_table)
      S3Transformations.transformIndexToRDD(parqDF, metaData)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   *
   * @param newPairs  The newly generated pairs
   * @param metaData  Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted
   */
  override def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData, intervals: List[Structs.Interval]): Unit = {
    Logger.getLogger("Index Table Write").log(Level.INFO, s"Start writing Index table")
    val start = System.currentTimeMillis()
    val previousIndexed = this.read_index_table(metaData, intervals) //read previously stored
    val combined = this.combine_index_table(newPairs, previousIndexed, metaData, intervals) //combine them
    metaData.pairs += newPairs.count() //update metadata
    val df = S3Transformations.transformIndexToDF(combined, metaData) //transform them
    //partition by the interval (start and end) and the first event of the event type pair
    df.repartition(col("interval"),col("eventA"))
      .select("interval.start", "interval.end", "eventA", "eventB", "occurrences")
      .write.partitionBy("start", "end", "eventA")
      .mode(SaveMode.Overwrite).parquet(this.index_table)
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Index Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Loads previously stored data in the CountTable
   *
   * @param metaData Object containing the metadata
   * @return The data stored in the count table
   */
  override def read_count_table(metaData: MetaData): RDD[Structs.Count] = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val df = spark.read.parquet(this.count_table)
      S3Transformations.transformCountToRDD(df)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
  }

  /**
   * Writes count to countTable
   *
   * @param counts   Calculated basic statistics per event type pair in order to be stored in the count table
   * @param metaData Object containing the metadata
   */
  override def write_count_table(counts: RDD[Structs.Count], metaData: MetaData): Unit = {
    Logger.getLogger("Count Table Write").log(Level.INFO, s" writing Count table")
    val start = System.currentTimeMillis()
    val previousIndexed = this.read_count_table(metaData) //read data
    val combined = this.combine_count_table(counts, previousIndexed, metaData) //combine them
    val df = S3Transformations.transformCountToDF(combined) //transform them
    //partition by the first event in the event type pair
    df.repartition(col("eventA"))
      .write.partitionBy("eventA")
      .mode(SaveMode.Overwrite).parquet(this.count_table)
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Count Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")

  }
}
