package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.S3ConnectorStreaming
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, KeyValueGroupedDataset, functions}

import java.sql.Timestamp
import scala.collection.mutable

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

    val kafkaBroker = "localhost:29092"

    val topic = "test"
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      //      .option("minOffsetsPerTrigger", 1000L) //process minimum 1000 events per batch
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


    //testing stream
    import spark.implicits._
    //    case class State(events: mutable.HashMap[String, List[Structs.EventWithPosition]], number_of_events: Int)
    //    implicit val stateEncoder: Encoder[State] = Encoders.bean(classOf[State])
    case class CustomState(events: mutable.HashMap[String, List[Structs.EventWithPosition]], number_of_events: Int) extends Serializable
    implicit val stateEncoder: Encoder[CustomState] = Encoders.kryo[CustomState]
    //    type State = Int

    def calculatePairs(traceId: Long, eventStream: Iterator[Structs.EventStream], groupState: GroupState[CustomState]): Iterator[Structs.SteamingPair] = {
      println("Hey")
      val values = eventStream.toSeq
      val initialState: CustomState = CustomState(new mutable.HashMap[String,List[Structs.EventWithPosition]],0)
      val oldState = groupState.getOption.getOrElse(initialState)


      val newValue = oldState.number_of_events + values.size
      val newState = CustomState(oldState.events,newValue)
      groupState.update(newState)

      Iterator(Structs.SteamingPair("", "", traceId, null, null, 0, 0))
    }

    //    val q3 = df_events
    //      .writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()


    val grouped: KeyValueGroupedDataset[Long, EventStream] = df_events.groupBy("trace").as[Long, EventStream]

    val z = grouped.flatMapGroupsWithState(OutputMode.Append,timeoutConf = GroupStateTimeout.NoTimeout)(calculatePairs)

    //    val z = grouped.mapGroups((k,iter)=>(k,iter.map(x=>x.event_type).toArray))
    val sq = z.
      writeStream
      .format("console")
      .outputMode("append")
      .start()

    //    val output: Dataset[Structs.SteamingPair] = grouped.flatMapGroupsWithState(OutputMode.Append,
    //      timeoutConf = GroupStateTimeout.NoTimeout)((trace_id, eventStream, s: GroupState[State]) => {
    //      println("Hey")
    //      val values = eventStream.toSeq
    //      val initialState: State = 0
    //      val oldState = s.getOption.getOrElse(initialState)
    //      val newValue = oldState + values.size
    //      val newState = newValue
    //      s.update(newState)
    //      Iterator(Structs.SteamingPair("", "", trace_id, null, null, 0, 0))
    //    })

    val p = s"""s3a://siesta-test/testing/pairs/"""
    //    val sq = output.
    //      writeStream
    //      .format("delta")
    //      .option("checkpointLocation", f"${p}_checkpoints/")
    //      .start(p)

//    val sq = output.
//      writeStream
//      .format("console")
//      //      .outputMode("append")
//      .start()

    q1.awaitTermination()
    q2.awaitTermination()
    sq.awaitTermination()


    println("Hey")


  }

}
