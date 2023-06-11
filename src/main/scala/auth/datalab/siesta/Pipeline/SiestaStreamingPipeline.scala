package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.{EventDeserializer, S3ConnectorStreaming}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import io.delta._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}

import scala.util.parsing.json._

object SiestaStreamingPipeline {

  def execute(c: Config): Unit = {

    // Till now only S3 with delta lake will be available
    val s3Connector = new S3ConnectorStreaming()
    // Get the streaming context
    val spark = s3Connector.get_spark_context(config = c)

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

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "test")
      .load()

    val schema = StructType(Seq(
      DataTypes.createStructField("trace", IntegerType, false),
      DataTypes.createStructField("event_type", StringType, false),
      DataTypes.createStructField("timestamp", TimestampType, false),
    ))

    val df_events = df.selectExpr("CAST(value AS STRING) as event")
      .select(functions.from_json(functions.column("event"), schema).as("json"))
      .select("json.*")
      .as(Encoders.bean(classOf[Structs.EventStream]))




    val t = s"""s3a://siesta/testing/seq/"""

    // writing in Sequence Table
    val q1 = df_events.writeStream
      .format("delta")
      .partitionBy("trace")
      .outputMode("append")
      .option("checkpointLocation", f"${t}_checkpoints/")
      .start(t)


    val s = s"""s3a://siesta/testing/single/"""
    //writing in Single Table
    val q2 = df_events
      .writeStream
      .format("delta")
      .partitionBy("event_type")
      .option("checkpointLocation", f"${s}_checkpoints/")
      .start(s)

    val q3 = df_events
      .writeStream
      .format("console")
      .outputMode("append")
      .start()



    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()


    println("Hey")


  }

}
