package auth.datalab.siesta.Pipeline

import auth.datalab.siesta.BusinessLogic.ExtractCounts.ExtractCounts
import auth.datalab.siesta.BusinessLogic.ExtractPairs.ExtractPairs
import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs.{InvertedSingleFull, LastChecked}
import auth.datalab.siesta.BusinessLogic.Model.{Event, EventTrait}
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import auth.datalab.siesta.DeclareIncrementa.DeclareIncrementalPipeline


object SiestaPipeline {

  def execute(c: Config): Unit = {


    //If new database is added the dbConnector can be set here.
    val dbConnector = new S3Connector()


    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)

    val spark = SparkSession.builder().getOrCreate()
    
    spark.time({
      val metadata:MetaData = dbConnector.get_metadata(c)
      val sequenceRDD: RDD[EventTrait] = if (!c.duration_determination) {
        IngestingProcess.getData(c).flatMap(_.events)
      } else {
        val detailedSequenceRDD: RDD[EventTrait] = IngestingProcess.getDataDetailed(c)
          .flatMap(_.events)
        dbConnector.write_sequence_table(detailedSequenceRDD, metadata, detailed = true)
        detailedSequenceRDD
      }

      implicit def ordered: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => {
        x compareTo y
      }

      val min_ts = sequenceRDD.map(x => Timestamp.valueOf(x.timestamp)).min()
      val max_ts = sequenceRDD.map(x => Timestamp.valueOf(x.timestamp)).max()
      //set min max ts in the metadata
      if (metadata.start_ts.equals("")){
        metadata.start_ts=min_ts.toString()
      }
      if(metadata.last_ts.equals("") || Timestamp.valueOf(metadata.last_ts).before(max_ts)){
        metadata.last_ts=max_ts.toString()
      }
        
      

      val ids_changed = sequenceRDD.map(_.trace_id).distinct().collect().toSet
      val bIds_changed = spark.sparkContext.broadcast(ids_changed)

      val prev_events = dbConnector.read_sequence_table(metaData = metadata)

      val fixed_positions: RDD[EventTrait] = if (prev_events != null) {
        val last_positions = prev_events
          .filter(x => bIds_changed.value.contains(x.trace_id))
          .groupBy(_.trace_id).map(x => (x._1, x._2.map(_.position).max + 1)).collectAsMap()
        val bLast_positions = spark.sparkContext.broadcast(last_positions)
        sequenceRDD.map(x => {
          val prev_pos = bLast_positions.value.getOrElse(x.trace_id, 0)
          new Event(trace_id = x.trace_id, timestamp = x.timestamp, event_type = x.event_type, position = x.position + prev_pos)
        })
      } else { //no need to fix
        sequenceRDD
      }

      //events with fixed positions are then stored to both sequence table and single table
      dbConnector.write_sequence_table(sequenceRDD = fixed_positions, metaData = metadata)
      dbConnector.write_single_table(sequenceRDD = fixed_positions, metaData = metadata)

      //read single table and filter all the events that appear in the traces that changed
      val singleRDD = dbConnector.read_single_table(metadata)
        .filter(x => bIds_changed.value.contains(x.trace_id))

      val inverted: RDD[InvertedSingleFull] = singleRDD
        .groupBy(x => (x.trace_id, x.event_type))
        .map(x => {
          val ts_s = x._2.map(y=>(y.position,y.timestamp)).toList.sortWith((a,b)=>a._1<b._1)
          InvertedSingleFull(x._1._1, x._1._2, ts_s.map(_._2), ts_s.map(_._1))
        })

      val lastChecked: RDD[LastChecked] = dbConnector.read_last_checked_table(metadata)

      //extract new pairs
      val pairs = ExtractPairs.extract(inverted, lastChecked, metadata.lookback)

      //merging last checked records
      val diffInMills = TimeUnit.DAYS.toMillis(metadata.lookback)
      val bDiffInMills = spark.sparkContext.broadcast(diffInMills)
      val bmin_ts = spark.sparkContext.broadcast(min_ts.getTime)

      val merged_rdd = if (lastChecked != null) {
        lastChecked.keyBy(x => (x.eventA, x.eventB, x.id))
          .fullOuterJoin(pairs._2.keyBy(x => (x.eventA, x.eventB, x.id)))
          .map(x => {
            if (x._2._2.isEmpty) {
              x._2._1.get
            } else {
              x._2._2.get
            }
          })
      } else {
        pairs._2
      }

      //removing last checked records that are more than 'lookback'- time ago

      val filtered_rdd = merged_rdd.filter(x => {
        val diff = bmin_ts.value - Timestamp.valueOf(x.timestamp).getTime
        diff <= 0 || (diff>0 && diff<bDiffInMills.value)
      })
//        .foreach(x=>(x.eventA,x.eventB,x.id,x.timestamp))
      Logger.getLogger("Last Checked records").log(Level.INFO, s"${filtered_rdd.count()}")

      //write merged last checked
      dbConnector.write_last_checked_table(filtered_rdd, metadata)
//      pairs._2.unpersist()
      //write new event pairs
      dbConnector.write_index_table(pairs._1, metadata)
//      pairs._1.unpersist()
      //calculate and write countTable
      val counts = ExtractCounts.extract(pairs._1)
//      counts.persist(StorageLevel.MEMORY_AND_DISK)
      dbConnector.write_count_table(counts, metadata)
//      counts.unpersist()

      //Update metadata before exiting
      metadata.has_previous_stored = true
      dbConnector.write_metadata(metadata)
    })

    // execute the Declare Incremental as a post processing step
    if(c.declare_incremental){
      spark.time({
        val metadata = dbConnector.get_metadata(c)
        DeclareIncrementalPipeline.execute(dbConnector,metadata)
      })
    }

    dbConnector.closeSpark()
  }

}
