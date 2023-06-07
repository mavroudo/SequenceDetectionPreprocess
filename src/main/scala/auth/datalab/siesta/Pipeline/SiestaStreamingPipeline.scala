package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.{EventDeserializer, S3ConnectorStreaming}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._

object SiestaStreamingPipeline {

  def execute(c: Config): Unit = {

    // Till now only S3 with delta lake will be available
    val s3Connector = new S3ConnectorStreaming()
    // Get the streaming context
    val sc = s3Connector.get_spark_context(config = c)

    val topicSet:Set[String] = Set("test")
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"localhost:29092",
      ConsumerConfig.GROUP_ID_CONFIG->"1",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[EventDeserializer]
    )
    val kafkaStream = KafkaUtils.createDirectStream(sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,EventStream](topicSet,kafkaParams))

    val lines = kafkaStream.map(x=>(x.key(),x.value()))
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    lines.print()


    sc.start()
    sc.awaitTermination()
    println("Hey")



  }

}
