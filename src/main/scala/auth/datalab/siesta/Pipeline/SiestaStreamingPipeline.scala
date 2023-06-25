package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.{Count, EventStream}
import auth.datalab.siesta.BusinessLogic.StreamingProcess.StreamingProcess
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.S3ConnectorStreaming
import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._

object SiestaStreamingPipeline {

  def execute(c: Config): Unit = {

    // Till now only S3 with delta lake will be available
    val s3Connector = new S3ConnectorStreaming()
    // Get the streaming context
    val spark = s3Connector.get_spark_context(config = c)
    import spark.implicits._
    //TODO: pass values by parameters
    val topicSet: Set[String] = Set("test")
    val kafkaBroker = "localhost:29092"

    val topic = "test"
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      .option("minOffsetsPerTrigger", "5") //process minimum 1000 events per batch
      .option("maxTriggerDelay", "10s") // if not the minOffsetPerTrigger reaches in 10s it will fire a trigger
      .load()

    val schema = StructType(Seq(
      DataTypes.createStructField("trace", IntegerType, false),
      DataTypes.createStructField("event_type", StringType, false),
      DataTypes.createStructField("timestamp", TimestampType, false),
    ))

    val df_events: Dataset[EventStream] = df.selectExpr("CAST(value AS STRING) as event")
      .select(functions.from_json(functions.column("event"), schema).as("json"))
      .select("json.*")
      .as(Encoders.bean(classOf[Structs.EventStream]))

    //TODO: filter and keep only the new ones

    //TODO: Set names of the tables  in the S3StreamingConnector (along with the creation of delta)
    val t = s"""s3a://siesta-test/testing/seq/"""

    // writing in Sequence Table
    val q1 = df_events.writeStream
      .format("delta")
      .queryName("Write SequenceTable")
      .partitionBy("trace")
      .outputMode("append")
      .option("checkpointLocation", f"${t}_checkpoints/")
      .start(t)


    val s = s"""s3a://siesta-test/testing/single/"""
    //writing in Single Table
    val q2 = df_events
      .writeStream
      .queryName("Write SingleTable")
      .format("delta")
      .partitionBy("event_type")
      .option("checkpointLocation", f"${s}_checkpoints/")
      .start(s)


    val grouped: KeyValueGroupedDataset[Long, EventStream] = df_events.groupBy("trace").as[Long, EventStream]
    val pairs: Dataset[Structs.StreamingPair] = grouped.flatMapGroupsWithState(OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculatePairs)

    val p = s"""s3a://siesta-test/testing/pairs/"""
    val sq = pairs.
      writeStream
      .queryName("Write IndexTable")
      .format("delta")
      .partitionBy("eventA")
      .option("checkpointLocation", f"${p}_checkpoints/")
      .start(p)


    val cp = s"""s3a://siesta-test/testing/counts/"""


    val counts: DataFrame = pairs.map(x => {
      Count(x.eventA, x.eventB, x.timeB.getTime - x.timeA.getTime, 1, x.timeB.getTime - x.timeA.getTime, x.timeB.getTime - x.timeA.getTime)
    }).groupBy("eventA", "eventB")
      .as[(String, String), Structs.Count]
      .flatMapGroupsWithState(OutputMode.Append, timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculateMetrics)
      .toDF()


    //create the CountTable, empty at the beginning so the merge can work
    val countSchema = StructType(Seq(
      StructField("eventA", StringType, nullable = false),
      StructField("eventB", StringType, nullable = false),
      StructField("sum_duration", LongType, nullable = false),
      StructField("count", IntegerType, nullable = false),
      StructField("min_duration", LongType, nullable = false),
      StructField("max_duration", LongType, nullable = false)
    ))
    // Check if the Delta table already exists
    val tableExists = DeltaTable.isDeltaTable(spark, cp)

    if (!tableExists) {
      // Create an empty DataFrame with the predefined schema
      val emptyDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], countSchema)

      // Write the empty DataFrame as a Delta table
      emptyDataFrame.write
        .format("delta")
        .mode("overwrite")
        .save(cp)

      println("Delta table created.")
    } else {
      println("Delta table already exists.")
    }


    val countTable = DeltaTable.forPath(spark, cp)
    val count = counts.writeStream
      .queryName("Write CountTable")
      .format("delta")
      .foreachBatch((microBatchOutputDF: DataFrame, batchId: Long) => {
        countTable.as("p")
          .merge(
            microBatchOutputDF.as("n"),
            "p.eventA = n.eventA and p.eventB = n.eventB")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
      })
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", f"${cp}_checkpoints/")
      .start()

//    val readCount = spark.
//      readStream
//      .format("delta")
//      .load(cp)
//      .writeStream
//      .format("console")
//      .outputMode(OutputMode.Append)
//      .start()


    println("Waiting for events...")
    q1.awaitTermination()
    q2.awaitTermination()
    sq.awaitTermination()
    count.awaitTermination()
//    readCount.awaitTermination()


  }

}
