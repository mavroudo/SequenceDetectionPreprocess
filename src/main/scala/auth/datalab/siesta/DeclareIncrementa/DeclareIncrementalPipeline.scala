package auth.datalab.siesta.DeclareIncrementa

import auth.datalab.siesta.S3Connector.S3Connector
import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.sql.SparkSession

object DeclareIncrementalPipeline {
  

    def execute(s3Connector:S3Connector):Unit={
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._
      //extract all events
      val all_events: Dataset[Event] = s3Connector.get_events_sequence_table() //TODO: fix this

      all_events.persist(StorageLevel.MEMORY_AND_DISK)

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
            Timestamp.valueOf(bPrevMining.value).before(Timestamp.valueOf(a.ts))
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

      //      val complete_pairs_that_changed = all_pairs
      //        .filter(functions.col("trace_id").isin(changedTraces:_*))

      complete_traces_that_changed.persist(StorageLevel.MEMORY_AND_DISK)
      //      complete_pairs_that_changed.persist(StorageLevel.MEMORY_AND_DISK)

      //extract positions
      val position_constraints = DeclareMining.extract_positions(new_events = new_events, logname = metaData.log_name,
        complete_traces_that_changed, bChangedTraces, support, metaData.traces)

      //extract existence
      val existence_constraints = DeclareMining.extract_existence(logname = metaData.log_name,
        complete_traces_that_changed = complete_traces_that_changed, bChangedTraces = bChangedTraces,
        support = support, total_traces = metaData.traces)

      //extract unordered
      val unordered_constraints = DeclareMining.extract_unordered(logname = metaData.log_name, complete_traces_that_changed,
        bChangedTraces, activity_matrix, support, metaData.traces)

      //extract order relations
      val ordered_constraints = DeclareMining.extract_ordered(metaData.log_name, complete_traces_that_changed, bChangedTraces,
        bEvent_types_occurrences, activity_matrix, support)

      //handle negative pairs = pairs that does not appear not even once in the data
      val negative_pairs: Array[(String, String)] = DeclareMining.handle_negatives(metaData.log_name,
        activity_matrix)

      val l = ListBuffer[String]()
      //each negative pair wil have 100% support in the constraints not-coexist, exclusive-choice, not succession and not chain-succession
      negative_pairs.foreach(x => {
        //      l+=s"not-chain-succession|${x._1}|${x._2}|1.000\n"
        l += s"not-succession|${x._1}|${x._2}|1.000\n"
      })
      negative_pairs.filter(x => x._1 < x._2).foreach(x => {
        if (negative_pairs.contains((x._2, x._1))) {
          l += s"not co-existence|${x._1}|${x._2}|1.000\n"
          l += s"exclusive choice|${x._1}|${x._2}|1.000\n"
        }

      })


      position_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.event_type}|$formattedDouble\n"
      })
      existence_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.event_type}|${x.n}|$formattedDouble\n"
      })
      unordered_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.eventA}|${x.eventB}|$formattedDouble\n"
      })
      ordered_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.eventA}|${x.eventB}|$formattedDouble\n"
      })

      val file = "output_first.txt"
      val writer = new BufferedWriter(new FileWriter(file))
      l.toList.foreach(writer.write)
      writer.close()

      if (!new_events.isEmpty) {
        val last_ts = new_events.rdd
          .map(x => Timestamp.valueOf(x.ts)).reduce((x, y) => {
            if (x.after(y)) {
              x
            } else {
              y
            }
          })
        metaData.last_declare_mined = last_ts.toString
        s3Connector.write_metadata(metaData)
      }
      //TODO: change support/ total traces to broadcasted variables

      all_events.unpersist()
      activity_matrix.unpersist()
      complete_traces_that_changed.unpersist()
      //      complete_pairs_that_changed.unpersist()



    }
}
