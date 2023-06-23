package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.{Count, EventStream}
import auth.datalab.siesta.BusinessLogic.StreamingProcess.StreamingProcess
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.S3ConnectorStreaming
import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, KeyValueGroupedDataset, SparkSession, functions}

import java.sql.Timestamp
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break

object SiestaStreamingPipeline {

  def execute(c: Config): Unit = {

    // Till now only S3 with delta lake will be available
    val s3Connector = new S3ConnectorStreaming()
    // Get the streaming context
    val spark = s3Connector.get_spark_context(config = c)
    import spark.implicits._
    val topicSet: Set[String] = Set("test")
    //    val kafkaParams = Map[String,Object](
    //      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"localhost:29092", //TODO: pass with env
    //      ConsumerConfig.GROUP_ID_CONFIG->"1",
    //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
    //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[EventDeserializer]
    //    )
    //    val kafkaStream = KafkaUtils.createDirectStream(sc,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe[String,EventStream](topicSet,kafkaParams))

    val kafkaBroker = "localhost:29092"

    val topic = "test"
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      //      .option("minOffsetsPerTrigger", "1000") //process minimum 1000 events per batch
      //      .option("maxTriggerDelay", "10s") // if not the minOffsetPerTrigger reaches in 10s it will fire a trigger
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



    val t = s"""s3a://siesta-test/testing/seq/"""

    // writing in Sequence Table
    val q1 = df_events.writeStream
      .format("delta")
      .partitionBy("trace")
      .outputMode("append")
      .option("checkpointLocation", f"${t}_checkpoints/")
      .start(t)


    val s = s"""s3a://siesta-test/testing/single/"""
    //writing in Single Table
    val q2 = df_events
      .writeStream
      .format("delta")
      .partitionBy("event_type")
      .option("checkpointLocation", f"${s}_checkpoints/")
      .start(s)



    val grouped: KeyValueGroupedDataset[Long, EventStream] = df_events.groupBy("trace").as[Long, EventStream]
    val pairs: Dataset[Structs.StreamingPair] = grouped.flatMapGroupsWithState(OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculatePairs)

    //    val sq = z.
    //      writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()
    //
    val p = s"""s3a://siesta-test/testing/pairs/"""
    val sq = pairs.
      writeStream
      .format("delta")
      .partitionBy("eventA")
      .option("checkpointLocation", f"${p}_checkpoints/")
      .start(p)




    val cp = s"""s3a://siesta-test/testing/counts/"""


    val counts:DataFrame = pairs.map(x=>{
      Count(x.eventA,x.eventB,x.timeB.getTime-x.timeA.getTime,1,x.timeB.getTime-x.timeA.getTime,x.timeB.getTime-x.timeA.getTime)
    }).groupBy("eventA","eventB")
      .as[(String,String),Structs.Count]
      .flatMapGroupsWithState(OutputMode.Append,timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculateMetrics)
      .toDF()

    val deltaTable = DeltaTable.forPath(spark, "/data/aggregates") //TODO:create an empty dataframe - or 1 dummy entrance

    val count  = counts.writeStream
      .format("delta")
      .foreachBatch((microBatchOutputDF:DataFrame,batchId:Long)=>{
        deltaTable.as("t")
          .merge(
            microBatchOutputDF.as("s"),
            "s.key = t.key")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
      })
      .outputMode(OutputMode.Update)
      .start()


//
//    def update(batchDF:Dataset[Count],batchId:Long)={
//      val cp = s"""s3a://siesta-test/testing/counts/"""
//      val spark = SparkSession.builder().getOrCreate()
//      val deltaTableExists = spark.catalog.tableExists(cp)
//      // Check if the Delta table exists
//      if (deltaTableExists) {
//        // Update the Delta table using merge
//        batchDF.createOrReplaceTempView("streaming_data")
//
//        val mergeQuery =
//          """
//          MERGE INTO delta_table t
//          USING streaming_data s
//          ON t.eventA = s.eventA AND t.eventB = s.eventB
//          WHEN MATCHED THEN UPDATE SET *
//          WHEN NOT MATCHED THEN INSERT *
//        """
//
//        spark.sql(mergeQuery)
//      } else {
//        // If the Delta table does not exist, write the batch data directly
//        batchDF.write.format("delta").save(cp)
//      }
//    }
//
//
//    val count = counts.writeStream
//      .foreachBatch(update _).start()

//  val count = counts.writeStream
//    .format("delta").outputMode(OutputMode.Complete)
//    .partitionBy("eventA","eventB")
//    .option("checkpointLocation", f"${cp}_checkpoints/")
//    .start(cp)

//    val count = counts.
//      writeStream
//      .format("console")
//      .outputMode("append")
////      .partitionBy("eventA")
////      .outputMode(OutputMode.Update())
////      .start(cp)
//      .start()

    q1.awaitTermination()
    q2.awaitTermination()
    sq.awaitTermination()
//    count.awaitTermination()



  }

}
