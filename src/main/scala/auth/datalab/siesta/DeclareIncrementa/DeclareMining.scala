package auth.datalab.siesta.DeclareIncrementa

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import auth.datalab.siesta.BusinessLogic.Model.Structs._
import auth.datalab.siesta.BusinessLogic.Model.Event
import auth.datalab.siesta.DeclareIncrementa.DeclareIncrementalPipeline.EventDeclare

import scala.collection.mutable.ListBuffer

object DeclareMining {


  /**
    * Extract the state for the position constraints. This state maintains the activities that appear first and last in traces.
    * This method is responsible to add the first events for new traces and update the last events of traces that contain new
    * events in the last batch. The state is stored in the declare/position.parquet file.
    *
    * @param new_events events in the new batch
    * @param logname name of the log database
    * @param complete_traces_that_changed all events of traces that changed in the new batch
    * @param bChangedTraces broadcasted variable in the form (trace_id)->[start_position, end_position]
    */
  def extract_positions(new_events: Dataset[EventDeclare], logname: String, complete_traces_that_changed: Dataset[EventDeclare],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val position_path = s"""s3a://siesta/$logname/declare/position.parquet/"""

    val previously = try {
      spark.read.parquet(position_path).as[PositionConstraint]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PositionConstraint]
    }

    val plusConstraint: Dataset[PositionConstraint] = new_events.map(x => {
        if (x.pos == 0) {
          Some(PositionConstraint("first", x.event_type, 1L))
        }
        else if (bChangedTraces.value.getOrElse(x.trace_id, (-1, -1))._2 == x.pos) { //this is the last event
          Some(PositionConstraint("last", x.event_type, 1L))
        }
        else {
          null
        }
      }).filter(_.isDefined)
      .map(_.get)

    val endPosMinus = complete_traces_that_changed
      .filter(x => {
        bChangedTraces.value(x.trace_id)._1 - 1 == x.pos
      })
      .map(x => PositionConstraint("last", x.event_type, -1L))

    val new_positions_constraints = plusConstraint.union(endPosMinus)
      .union(previously)
      .rdd
      .keyBy(x => (x.rule, x.event_type))
      .reduceByKey((x, y) => PositionConstraint(x.rule, x.event_type, x.occurrences + y.occurrences))
      .map(_._2)

    new_positions_constraints.count()
    new_positions_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    new_positions_constraints.toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(position_path)
  }



  /**
    * Extract the state for the existence constraints. This state maintains the number of traces that contain a specific number of
    * occurrences per activity. For example, there is a record that shows how many trace contain exactly 2 occurrences of the
    * activity A. This state is then used to calculate existence constraints and it is stored in the declare/existence.parquet file.
    *
    * @param logname name of the log database
    * @param complete_traces_that_changed all events of traces that changed in the new batch
    * @param bChangedTraces broadcasted variable in the form (trace_id)->[start_position, end_position]
    */
  def extract_existence(logname: String, complete_traces_that_changed: Dataset[EventDeclare],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val existence_path = s"""s3a://siesta/$logname/declare/existence.parquet/"""

    val previously = try {
      spark.read.parquet(existence_path)
        .map(x => ActivityExactly(x.getString(0), x.getInt(1), x.getLong(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[ActivityExactly]
    }

    //extract changes from the new ones: add news and remove previous records
    val changesInConstraints = complete_traces_that_changed
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => {
        val added_events = bChangedTraces.value(t._1) //gets starting and ending position of the new events

        val all_trace: Map[String, Int] = t._2.map(e => (e.event_type, 1)).groupBy(_._1).mapValues(_.size)

        val previousValues: Map[String, Int] = if (added_events._1 != 0) { //there are previous events from this trace
          t._2.filter(_.pos < added_events._1).map(e => (e.event_type, 1)).groupBy(_._1).mapValues(_.size)
        } else {
          Map.empty[String, Int]
        }
        val l = ListBuffer[ActivityExactly]()
        all_trace.foreach(aT => {
          l += ActivityExactly(aT._1, aT._2, 1L)
          if (previousValues.contains(aT._1)) { // this activity existed at least once in the previous trace
            l += ActivityExactly(aT._1, previousValues(aT._1), -1L)
          }
        })
        l.toList
      })

    val merged_constraints = changesInConstraints.union(previously.rdd)
      .keyBy(x => (x.event_type, x.occurrences))
      .reduceByKey((x, y) => ActivityExactly(x.event_type, x.occurrences, x.contained + y.contained))
      .map(_._2)
      .toDS()

    merged_constraints.count()
    merged_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    merged_constraints.toDF()
      .write.mode(SaveMode.Overwrite)
      .parquet(existence_path)
  }

  /**
    * Extract the state for the unordered constraints. This state maintains the tables U[x] and |I[x,y]UI[y,x]|, which are
    * then used to extract all unordered constraints. The tables are stored in the /declare/unorder/u.parquet/ and
    * /declare/unorder/i.parquet/ files, respectively.
    *
    * @param logname name of the log database
    * @param complete_traces_that_changed all events of traces that changed in the new batch
    * @param bChangedTraces broadcasted variable in the form (trace_id)->[start_position, end_position]
    */
  def extract_unordered(logname: String, complete_traces_that_changed: Dataset[EventDeclare],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous U[x] if exist
    val u_path = s"""s3a://siesta/$logname/declare/unorder/u.parquet/"""

    val previously = try {
      spark.read.parquet(u_path)
        .map(x => (x.getString(0), x.getLong(1)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, Long)]
    }

    //calculate new event types in the traces that changed
    val u_new: RDD[(String, Long)] = complete_traces_that_changed //this will produce rdd (event_type, #new occurrences)
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => { //find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        val prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.event_type).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) //+1 because last one is exclusive
          .map(_.event_type)
          .filter(e => !prevEvents.contains(e))
          .distinct
          .map(e => (e, 1L)) //mapped to event type,1 (they will be counted later)
      })
      .keyBy(_._1)
      .reduceByKey((a, b) => (a._1, a._2 + b._2))
      .map(_._2)

    val merge_u = previously.rdd.fullOuterJoin(u_new)
      .map(x => {
        val total = x._2._1.getOrElse(0L) + x._2._2.getOrElse(0L)
        (x._1, total)
      })
      .toDS()

    merge_u.count()
    merge_u.persist(StorageLevel.MEMORY_AND_DISK)
    merge_u.write.mode(SaveMode.Overwrite).parquet(u_path)

    //get previous |I[a,b]UI[b,a]| if exist
    val i_path = s"""s3a://siesta/$logname/declare/unorder/i.parquet/"""

    val previously_i = try {
      spark.read.parquet(i_path)
        .map(x => (x.getString(0), x.getString(1), x.getLong(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, String, Long)]
    }

    val new_pairs = complete_traces_that_changed
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => { // find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        var prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.event_type).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        val new_events = t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) // +1 because last one is exclusive
          .map(_.event_type)
          .toSet

        val l = ListBuffer[(String, String, Long)]()

        // Iterate over new events and previous events
        for (e1 <- new_events) {
          if (!prevEvents.contains(e1)){
            for (e2 <- prevEvents) {
              if (e1 < e2) {
                l += ((e1, e2, 1L))
              } // Add to the list if e1 < e2 and they're unique
              else if (e2 < e1) {
                l += ((e2, e1, 1L))
              }
            }
            prevEvents = prevEvents + e1 // Return a new Set with e1 added
          }
        }

        l.toList
      })
      .keyBy(x => (x._1, x._2))
      .reduceByKey((x, y) => (x._1, x._2, x._3 + y._3))

    val merge_i = new_pairs
      .fullOuterJoin(previously_i.rdd.keyBy(x => (x._1, x._2)))
      .map(x => {
        val total = x._2._1.getOrElse(("", "", 0L))._3 + x._2._2.getOrElse(("", "", 0L))._3
        (x._1._1, x._1._2, total)
      })
      .toDS()
    merge_i.count()
    merge_i.persist(StorageLevel.MEMORY_AND_DISK)
    merge_i.write.mode(SaveMode.Overwrite).parquet(i_path)

    merge_u.unpersist()
    merge_i.unpersist()
    new_pairs.unpersist()
  }

  
  /**
    * Extract the state for the ordered constraints. This state maintains the values for some key constraints, which
    * can then be used to calculate the support of all the ordered constraints. The basic ordered constraints are
    * stored in the /declare/order.parquet/ file.
    *
    * @param logname name of the log database
    * @param complete_traces_that_changed all events of traces that changed in the new batch
    * @param bChangedTraces broadcasted variable in the form (trace_id)->[start_position, end_position]
    */
  def extract_ordered(logname: String, complete_traces_that_changed: Dataset[EventDeclare],
                      bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val order_path = s"""s3a://siesta/$logname/declare/order.parquet/"""

    val previously = try {
      spark.read.parquet(order_path)
        .map(x => PairConstraint(x.getString(0), x.getString(1), x.getString(2), x.getDouble(3)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraint]
    }

    val new_ordered_relations = complete_traces_that_changed.rdd
      .groupBy(_.trace_id)
      .flatMap(t => {
        val new_pos = bChangedTraces.value(t._1)
        //activities -> reverse sorted positions of the events that correspond to them
        val events: Map[String, Seq[Int]] = t._2.map(e => (e.event_type, e.pos))
          .groupBy(_._1)
          .mapValues(e => e.map(_._2).toSeq.sortWith((a, b) => a > b))
        val l = ListBuffer[PairConstraint]()
        // new events are the ones that will extract the new rules
        // precedence loop
        for (i <- new_pos._1 to new_pos._2) {
          val current_event_type = t._2.toSeq(i).event_type
          val all_previous = events(current_event_type).filter(x => x < i)
          val prev = all_previous match {
            case Nil => 0
            case _ => all_previous.max
          }
          // precedence loop
          events.keySet.filter(x => x != current_event_type)
            .foreach(activity_a => {
              val valid_a = events(activity_a)
                .filter(pos_a => pos_a < i)
              if (valid_a.nonEmpty) {
                val pos_a = valid_a.max
                if (pos_a < i) {
                  l += PairConstraint("precedence", activity_a, current_event_type, 1.0)
                  if (pos_a >= prev) {
                    l += PairConstraint("alternate-precedence", activity_a, current_event_type, 1.0)
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-precedence", activity_a, current_event_type, 1.0)
                    }
                  }
                }
              }
            })

          //response loop
          events.keySet.filter(x => x != current_event_type)
            .foreach(activity_a => {
              var largest = true
              events(activity_a).foreach(pos_a => {
                if (pos_a < i && pos_a >= prev) {
                  l += PairConstraint("response", activity_a, current_event_type, 1.0)
                  if (largest) {
                    l += PairConstraint("alternate-response", activity_a, current_event_type, 1.0)
                    largest = false
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-response", activity_a, current_event_type, 1.0)
                    }
                  }
                }
              })
            })
        }
        l.toList
      })
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .reduceByKey((x, y) => PairConstraint(x.rule, x.eventA, x.eventB, x.occurrences + y.occurrences))
      .map(_._2)

    //merge with previous values if exist
    val updated_constraints = new_ordered_relations
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .fullOuterJoin(previously.rdd.keyBy(x => (x.rule, x.eventA, x.eventB)))
      .map(x => PairConstraint(x._1._1, x._1._2, x._1._3,
        x._2._1.getOrElse(PairConstraint("", "", "", 0L)).occurrences +
          x._2._2.getOrElse(PairConstraint("", "", "", 0L)).occurrences))
      .toDS()

    updated_constraints.count()
    updated_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    //    write updated constraints back to s3
    updated_constraints.write.mode(SaveMode.Overwrite).parquet(order_path)
    updated_constraints.unpersist()
  }

  /**
    * Extracts the negative state, which contains all the pair of activities that do not appear in this order
    * in any of the traces.
    *
    * @param logname name of the log database
    * @param activity_matrix an rdd with all the possible pair of activities
    */
  def handle_negatives(logname: String, activity_matrix: RDD[(String, String)]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val negative_path = s"""s3a://siesta/$logname/declare/negatives.parquet/"""

    val order_path = s"""s3a://siesta/$logname/declare/order.parquet/"""

    val ordered_response = try {
      spark.read.parquet(order_path)
        .filter(x => x.getString(0) == "response" && x.getDouble(3) > 0)
        .map(x => (x.getString(1), x.getString(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, String)]
    }

    val new_negative_pairs = activity_matrix.subtract(ordered_response.rdd)
      .filter(x => x._1 != x._2)
      .toDS()

    new_negative_pairs.count()
    new_negative_pairs.persist(StorageLevel.MEMORY_AND_DISK)

    //    write new ones back to S3
    new_negative_pairs.write.mode(SaveMode.Overwrite).parquet(negative_path)
    new_negative_pairs.unpersist()
  }
}
