package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.BusinessLogic.StreamingProcess.StreamingProcess
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.S3ConnectorStreaming
import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.types._

import java.time.Duration


object SiestaStreamingPipeline {

  private def stopQuery(query : StreamingQuery): Unit = {
    if(query.isActive && !query.status.isDataAvailable && !query.status.isTriggerActive) {
      println(s"Terminating Query ${query.name}")
      query.stop()
    }
  }

  def execute(c: Config): Unit = {

    // Till now only S3 with delta lake will be available
    val s3Connector = new S3ConnectorStreaming()
    // Get the streaming context
    val spark = s3Connector.get_spark_context(config = c)
    s3Connector.initialize_db(config = c)

    import spark.implicits._

    val kafkaBroker = "localhost:29092"
    val topic = "test"
//    val kafkaBroker = Utilities.readEnvVariable("kafkaBroker")
//    val topic = Utilities.readEnvVariable("kafkaTopic")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      //      .option("minOffsetsPerTrigger", "5") //process minimum 1000 events per batch
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

    // writing in Sequence Table
    val sequenceTableQueries = s3Connector.write_sequence_table(df_events)

    //writing in Single Table
    val singleTableQuery = s3Connector.write_single_table(df_events)

    //Compute pairs using Stateful function
    val duration = Duration.ofDays(c.lookback_days)
    val grouped: KeyValueGroupedDataset[Long, EventStream] = df_events.groupBy("trace").as[Long, EventStream]
    val pairs: Dataset[Structs.StreamingPair] = grouped.flatMapGroupsWithState(OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(StreamingProcess.calculatePairs)
      .filter(x => {
        val diff = x.timeB.getTime - x.timeA.getTime
        diff > 0 && diff < duration.toMillis
      })
    //writing in IndexTable
    val indexTableQueries = s3Connector.write_index_table(pairs)

    //write in CountTable
    val countTableQuery = s3Connector.write_count_table(pairs)


    spark.streams.addListener(new StreamingQueryListener() {
      var totalEvents = 0L

      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      }


      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if (queryProgress.progress.numInputRows > 0 && queryProgress.progress.name=="Write SingleTable") {
          totalEvents+= queryProgress.progress.numInputRows
          println(s"Processed events: ${totalEvents}")
        }

        if(totalEvents>=c.streaming_events){
          stopQuery(sequenceTableQueries._1)
          stopQuery(sequenceTableQueries._2)
          stopQuery(singleTableQuery)
          stopQuery(indexTableQueries._1)
          stopQuery(indexTableQueries._2)
          stopQuery(countTableQuery)
        }
      }
    })


    sequenceTableQueries._1.awaitTermination()
    sequenceTableQueries._2.awaitTermination()
    singleTableQuery.awaitTermination()
    indexTableQueries._1.awaitTermination()
    indexTableQueries._2.awaitTermination()
    countTableQuery.awaitTermination()




  }

}
