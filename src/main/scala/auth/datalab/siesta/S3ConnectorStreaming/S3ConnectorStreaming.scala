package auth.datalab.siesta.S3ConnectorStreaming

import auth.datalab.siesta.BusinessLogic.Metadata.{MetaData, SetMetadata}
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.BusinessLogic.StreamingProcess.StreamingProcess
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.Utils.Utilities
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.net.URI
import java.sql.SQLException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class S3ConnectorStreaming {
  var seq_table: String = _
  var delta_meta_table: String = _
  var single_table: String = _
  var last_checked_table: String = _
  var index_table: String = _
  var count_table: String = _
  var metadata: MetaData = _
  val unique_traces: mutable.Set[Long] = mutable.HashSet[Long]()

  /**
   * Initializes the private object of this class along with any table required in S3
   */
  def initialize_db(config: Config): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val bucket = "siesta-test"
    val fs = FileSystem.get(new URI(s"s3a://$bucket/"), spark.sparkContext.hadoopConfiguration)
    //initialize table names base don the given logname
    delta_meta_table = s"""s3a://$bucket/${config.log_name}/meta/"""
    seq_table = s"""s3a://$bucket/${config.log_name}/seq/"""
    single_table = s"""s3a://$bucket/${config.log_name}/single/"""
    index_table = s"""s3a://$bucket/${config.log_name}/index/"""
    count_table = s"""s3a://$bucket/${config.log_name}/count/"""

    //check for delete previous
    if (config.delete_previous) {
      fs.delete(new Path(s"""s3a://$bucket/${config.log_name}/"""), true)
    }
    //delete all stored indices in this db
    if (config.delete_all) {
      for (l <- fs.listStatus(new Path(s"""s3a://$bucket/"""))) {
        fs.delete(l.getPath, true)
      }
    }

    val connection = DatabaseConnector.getConnection
    try {
      DatabaseConnector.createTableIfNotExists(connection)
    } finally {
      if (connection != null) connection.close()
    }

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("\"" + queryStarted.name + "\" started at " + queryStarted.timestamp)
        val formatter = DateTimeFormatter.ISO_DATE_TIME

        val connection = DatabaseConnector.getConnection
        try {
          val sql = "INSERT INTO logs (query_name, ts, batch_id, batch_duration, num_input_rows) VALUES (?, ?, ?, ?, ?)"
          val statement = connection.prepareStatement(sql)
          statement.setString(1, queryStarted.name)

          val zonedDateTime = ZonedDateTime.parse(queryStarted.timestamp, formatter)
          val timestamp = zonedDateTime.toInstant.toEpochMilli
          statement.setLong(2, timestamp)
          statement.setLong(3, 0)
          statement.setLong(4, 0)
          statement.setLong(5, 0)
          statement.executeUpdate()
        } catch {
          case e: SQLException => e.printStackTrace() // Handle exceptions properly in production code
        } finally {
          if (connection != null) connection.close()
        }

      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
        if (queryTerminated.exception.nonEmpty) {
          println("Termination reason: " + queryTerminated.exception.get)
        }
      }


      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if (queryProgress.progress.numInputRows > 0) {
          val formatter = DateTimeFormatter.ISO_DATE_TIME

          val connection = DatabaseConnector.getConnection
          try {
            val sql = "INSERT INTO logs (query_name, ts, batch_id, batch_duration, num_input_rows) VALUES (?, ?, ?, ?, ?)"
            val statement = connection.prepareStatement(sql)
            statement.setString(1, queryProgress.progress.name)

            val zonedDateTime = ZonedDateTime.parse(queryProgress.progress.timestamp, formatter)
            val timestamp = zonedDateTime.toInstant.toEpochMilli
            statement.setLong(2, timestamp)
            statement.setLong(3, queryProgress.progress.batchId)
            statement.setLong(4, queryProgress.progress.batchDuration)
            statement.setLong(5, queryProgress.progress.numInputRows)
            statement.executeUpdate()
          } catch {
            case e: SQLException => e.printStackTrace() // Handle exceptions properly in production code
          } finally {
            if (connection != null) connection.close()
          }
          println(s" ${queryProgress.progress.name}  , handled batch ${queryProgress.progress.batchId.toString} in" +
            s" ${queryProgress.progress.batchDuration.toString}. Total rows processed ${queryProgress.progress.numInputRows.toString}")
        }
      }
    })


    //get metadata
    this.metadata = this.get_metadata(config)

    //create the CountTable, empty at the beginning so the merge can work
    val countSchema = StructType(Seq(
      StructField("eventA", StringType, nullable = false),
      StructField("eventB", StringType, nullable = false),
      StructField("sum_duration", LongType, nullable = false),
      StructField("count", IntegerType, nullable = false),
      StructField("min_duration", LongType, nullable = false),
      StructField("max_duration", LongType, nullable = false)
    ))
    val tableExists = DeltaTable.isDeltaTable(spark, count_table)
    if (!tableExists) {
      // Create an empty DataFrame with the predefined schema
      val emptyDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], countSchema)

      // Write the empty DataFrame as a Delta table
      emptyDataFrame.write
        .format("delta")
        .mode("overwrite")
        .save(count_table)
      Logger.getLogger("CountTable").log(Level.INFO, s"CountTable has been created")
    } else {
      Logger.getLogger("CountTable").log(Level.INFO, s"CountTable already exists, moving on")
    }
  }

  def get_spark_context(config: Config): SparkSession = {

    val s3accessKeyAws = Utilities.readEnvVariable("s3accessKeyAws")
    val s3secretKeyAws = Utilities.readEnvVariable("s3secretKeyAws")
    val s3ConnectionTimeout = Utilities.readEnvVariable("s3ConnectionTimeout")
    val s3endPointLoc: String = Utilities.readEnvVariable("s3endPointLoc")

    val conf = new SparkConf()
      .setAppName("Siesta incremental")
//      .setMaster("local[*]")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.parquet.compression.codec", config.compression)
      .set("spark.sql.parquet.filterPushdown", "true")


    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")


    spark
  }

  /**
   * Loads metadata from S3 if they exist or create a new object based on the  configuration object
   *
   * @param config The configuration parsed from the command line in Main
   * @return The Metadata object containing all the information for indexing
   */
  private def get_metadata(config: Config): MetaData = {
    Logger.getLogger("Metadata").log(Level.INFO, s"Getting metadata")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()
    val schema = ScalaReflection.schemaFor[MetaData].dataType.asInstanceOf[StructType]
    val metaDataObj = try {
      //      spark.read.schema(schema).json(delta_meta_table)
      spark.read.format("delta").load(delta_meta_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Metadata").log(Level.INFO, s"finished in ${total / 1000} seconds")
    val metaData = if (metaDataObj == null) {
      SetMetadata.initialize_metadata(config)
    } else {
      SetMetadata.load_metadata_delta(metaDataObj)
    }
    metaData
  }


  /**
   * Handles the writing of the events in the SequenceTable. There are 2 Streaming Queries.
   * The first one is to continuous write the incoming events in the delta table and the second
   * one is responsible to update the number of events and distinct traces in the metadata.
   *
   * @param df_events The events stream as it is read from the input source
   * @return References to the two queries, in order to use awaitTermination at the end of all
   *         the processes
   */
  def write_sequence_table(df_events: Dataset[EventStream]): (StreamingQuery, StreamingQuery) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sequenceTable = df_events
      .writeStream
      .format("delta")
      .queryName("Write SequenceTable")
      .partitionBy("trace")
      .outputMode("append")
      .option("checkpointLocation", f"${seq_table}_checkpoints/")
      .start(seq_table)


    val countEvents = df_events.toDF()
      .writeStream
      .queryName("Update metadata from SequenceTable")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        val batchEventCount = batchDF.count()
        batchDF.select("trace").as[Long].distinct().collect()
          .foreach(x => unique_traces.add(x))
        metadata.events = metadata.events + batchEventCount
        metadata.traces = unique_traces.size
//        updateMetadata()
      })
      .start()


    (sequenceTable, countEvents)

  }

  /**
   * Handles the writing of the events in the SingleTable. The original events are partitioned based
   * on the event type and then written in the delta table.
   *
   * @param df_events The events stream as it is read from the input source
   * @return Reference to the query, in order to use awaitTermination at the end of all the processes
   */
  def write_single_table(df_events: Dataset[EventStream]): StreamingQuery = {
    val singleTable = df_events
      .writeStream
      .queryName("Write SingleTable")
      .format("delta")
      .partitionBy("event_type")
      .option("checkpointLocation", f"${single_table}_checkpoints/")
      .start(single_table)
    singleTable
  }

  /**
   * Handles the writing of the event pairs in the IndexTable. The pairs have been calculated in the
   * [[auth.datalab.siesta.Pipeline.SiestaStreamingPipeline]] using a stateful function.
   *
   * @param pairs The calculated pairs of the last batch
   * @return Reference to the query, in order to use awaitTermination at the end of all the processes
   */
  def write_index_table(pairs: Dataset[Structs.StreamingPair]): (StreamingQuery, StreamingQuery) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val indexTable = pairs
      .writeStream
      .queryName("Write IndexTable")
      .format("delta")
      .partitionBy("eventA")
      .option("checkpointLocation", f"${index_table}_checkpoints/")
      .start(index_table)

    val logging = pairs.toDF().writeStream
      .queryName("Update metadata from IndexTable")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        val countPairs = batchDF.count()
        metadata.pairs = metadata.pairs + countPairs
        updateMetadata()
      })
      .start()
    (indexTable, logging)
  }

  /**
   * Handles the writing of the metrics for each event pair in the CountTable. This is the most complecated
   * query in the streaming mode, because metrics should overwrite the previous records
   *
   * @param pairs The calculated pairs for the last batch
   * @return Reference to the query, in order to use awaitTermination at the end of all the processes
   */
  def write_count_table(pairs: Dataset[Structs.StreamingPair]): StreamingQuery = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //calculate metrics for each pair
    val counts: DataFrame = pairs.map(x => {
        Structs.Count(x.eventA, x.eventB, x.timeB.getTime - x.timeA.getTime, 1,
          x.timeB.getTime - x.timeA.getTime, x.timeB.getTime - x.timeA.getTime)
      }).groupBy("eventA", "eventB")
      .as[(String, String), Structs.Count]
      .flatMapGroupsWithState(OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculateMetrics)
      .toDF()
    //write the calculated metrics in s3, making sure that the old ones will be overwritten
    val countTable = DeltaTable.forPath(spark, count_table)
    counts.writeStream
      .queryName("Write CountTable")
      .format("delta")
      .foreachBatch((microBatchOutputDF: DataFrame, batchId: Long) => {
        val c = microBatchOutputDF.count()
        countTable.as("p")
          .merge(
            microBatchOutputDF.as("n"),
            "p.eventA = n.eventA and p.eventB = n.eventB")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
      })
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", f"${count_table}_checkpoints/")
      .start()
  }

  private def updateMetadata(): Unit = {
    if (!DeltaTable.isDeltaTable(delta_meta_table)) {
      SetMetadata.transformToDF(metadata).write.format("delta").mode(SaveMode.Overwrite).save(delta_meta_table)
    } else {

      val deltaTable = DeltaTable.forPath(delta_meta_table)
      deltaTable.as("oldData").merge(SetMetadata.transformToDF(metadata).as("newData"), "oldData.key=newData.key")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }


}
