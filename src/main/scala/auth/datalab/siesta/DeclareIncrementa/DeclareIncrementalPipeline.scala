package auth.datalab.siesta.DeclareIncrementa

import auth.datalab.siesta.S3Connector.S3Connector
import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import org.apache.spark.storage.StorageLevel
import auth.datalab.siesta.BusinessLogic.Model.{Event,EventTrait}
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import org.apache.spark.sql.{Encoders,Encoder,functions}
import org.apache.spark.rdd.RDD
import java.sql.Timestamp

object DeclareIncrementalPipeline {

  case class EventDeclare(trace_id:String, timestamp: String, event_type: String, pos: Int)
 

    def execute(dbConnector:DBConnector, metaData:MetaData):Unit={
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._


      //extract all events
      val all_events = dbConnector.read_sequence_table(metaData)
        .map(x=>x.asInstanceOf[Event])
        .map(x=>(x.trace_id,x.timestamp,x.event_type,x.position))
        .toDF("trace_id","timestamp","event_type","pos")
        .as[EventDeclare]

      //extract event_types -> #occurrences
      val event_types_occurrences: scala.collection.Map[String, Long] = all_events      
        .select("event_type", "trace_id")
        .groupBy("event_type")
        .agg(functions.count("trace_id").alias("unique"))
        .collect()
        .map(row => (row.getAs[String]("event_type"), row.getAs[Long]("unique")))
        .toMap
      val bEvent_types_occurrences = spark.sparkContext.broadcast(event_types_occurrences)

      //all possible activity pair matrix
      val activity_matrix: RDD[(String, String)] = Utilities.get_activity_matrix(event_types_occurrences)
      activity_matrix.persist(StorageLevel.MEMORY_AND_DISK)

      //keep only the recently added events
      val bPrevMining = spark.sparkContext.broadcast(metaData.last_declare_mined)
      val new_events = all_events
        .filter(a => {
          if (bPrevMining.value == "") {
            true
          } else {
            // the events that//    new_events.show() we need to keep are after the previous timestamp
            Timestamp.valueOf(bPrevMining.value).before(Timestamp.valueOf(a.timestamp))
          }
        })

      
      println(new_events.count())
      println(bPrevMining.value)

      //maintain the traces that changed
      val changed_traces: scala.collection.Map[String, (Int, Int)] = new_events
        .groupBy("trace_id")
        .agg(functions.min("pos"), functions.max("pos"))
        .map(x => (x.getAs[String]("trace_id"), (x.getAs[Int]("min(pos)"), x.getAs[Int]("max(pos)"))))
        .rdd
        .keyBy(_._1)
        .mapValues(_._2)
        .collectAsMap()

      val bChangedTraces = spark.sparkContext.broadcast(changed_traces)

      val changedTraces = bChangedTraces.value.keys.toSeq
      //maintain from the original events those that belong to a trace that changed
      val complete_traces_that_changed = all_events
        .filter(functions.col("trace_id").isin(changedTraces:_*))
      complete_traces_that_changed.count()

      complete_traces_that_changed.persist(StorageLevel.MEMORY_AND_DISK)

      //extract position state
      DeclareMining.extract_positions(new_events, metaData.log_name, complete_traces_that_changed, bChangedTraces)

      //extract existence state
      DeclareMining.extract_existence(metaData.log_name, complete_traces_that_changed, bChangedTraces)

      //extract unordered state
      DeclareMining.extract_unordered(metaData.log_name, complete_traces_that_changed, bChangedTraces)

      //extract ordered state
      DeclareMining.extract_ordered(metaData.log_name, complete_traces_that_changed, bChangedTraces)

      //handle negative pairs = pairs that does not appear not even once in the data
      DeclareMining.handle_negatives(metaData.log_name, activity_matrix)


      metaData.last_declare_mined = metaData.last_ts
      dbConnector.write_metadata(metaData)

      all_events.unpersist()
      activity_matrix.unpersist()
      complete_traces_that_changed.unpersist()

    }
}
