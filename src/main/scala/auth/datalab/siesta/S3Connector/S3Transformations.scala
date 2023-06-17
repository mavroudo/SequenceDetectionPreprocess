package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp

/**
 * This class is responsible to transform the loaded data from S3 into RDDs to processed by the main pipeline.
 * Then transform the RDDs back to Dataframes, in order to utilize the spark api to store them back in S3.
 * Therefore there are 2 transformations for each table, one to load data and one to write them back.
 */
object S3Transformations {

  /**
   * The required format is [trace_id, List of events], where each event is [event_type, timestamp]
   * @param sequenceRDD The RDD containing the traces
   * @return The transformed Dataframe
   */
  def transformSeqToDF(sequenceRDD: RDD[Structs.Sequence]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    sequenceRDD.map(x => {
      (x.sequence_id, x.events.map(y => (y.event, y.timestamp)))
    }).toDF("trace_id", "events")
  }

  /**
   * The stored format is [trace_id, List of events], where each event is [event_type, timestamp]
   * @param df The loaded traces from S3
   * @return The transformed RDD of traces
   */
  def transformSeqToRDD(df: DataFrame): RDD[Structs.Sequence] = {
    df.rdd.map(x => {
      val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
      val concat = pEvents.map(x => Structs.Event(x._2, x._1))
      Structs.Sequence(concat.toList, x.getAs[Long]("trace_id"))
    })
  }

  /**
   * The stored format is [event_type, List of occurrences], where each occurrence contains the occurrences of this event type
   * in a particular trace. Therefore an occurrence has the structure [trace_id, list of timestamps, list of position],
   * where the timestamps and positions correspond to the where and when the event type occurred in the trace.
   * @param df The loaded single inverted index from S3
   * @return The transformed RDD
   */
  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
    df.rdd.flatMap(x => {
      val event_name = x.getAs[String]("event_type")
      val occurrences = x.getAs[Seq[Row]]("occurrences").map(y => (y.getLong(0), y.getAs[Seq[String]](1), y.getAs[Seq[Int]](2)))
      occurrences.map(o => {
        Structs.InvertedSingleFull(o._1, event_name, o._2.toList, o._3.toList)
      })
    })
  }

  /**
   * The required format is [event_type, List of occurrences], where each occurrence contains the appearences of this event type
   * in a particular trace. Therefore an occurrence has the structure [trace_id, list of timestamps, list of position],
   * where the timestamps and positions correspond to the where and when the event type occurred in the trace.
   *
   * @param singleRDD The RDD containing the single inverted index
   * @return The transformed Dataframe
   */
  def transformSingleToDF(singleRDD: RDD[Structs.InvertedSingleFull]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    singleRDD.groupBy(_.event_name)
      .map(y => {
        Structs.InvertedSingle(y._1, y._2.map(x => Structs.IdTimePositionList(x.id, x.times, x.positions)).toList)

      }).toDF("event_type", "occurrences")
  }

  /**
   * The stored format is [event_typeA, event_typeB, List of occurrences], where each occurrence has the structure:
   * [trace_id, last timestamp]. So for each event type pair we maintain the information when was the last time that
   * this event occurred in each trace. This information is latter utilized to facilitate in the efficient incremental
   * indexing
   * @param df The loaded LastChecked information from S3
   * @return The transformed RDD
   */
  def transformLastCheckedToRDD(df: DataFrame): RDD[LastChecked] = {
    df.rdd.flatMap(x => {
      val eventA = x.getAs[String]("eventA")
      val eventB = x.getAs[String]("eventB")
      x.getAs[Seq[Row]]("occurrences").map(oc => {
        LastChecked(eventA, eventB, oc.getLong(0), oc.getString(1))
      })
    })
  }

  /**
   * The required format is [event_typeA, event_typeB, List of occurrences], where each occurrence has the structure:
   * [trace_id, last timestamp]. So for each event type pair we maintain the information when was the last time that
   * this event occurred in each trace. This information is latter utilized to facilitate in the efficient incremental
   * indexing
   *
   * @param lastchecked The RDD containing the last timestamps for each event type pair per trace
   * @return The transformed Dataframe
   */
  def transformLastCheckedToDF(lastchecked: RDD[LastChecked]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    lastchecked.groupBy(x => (x.eventA, x.eventB))
      .map(x => {
        val occurrences = x._2.map(y => Structs.IdTime(y.id, y.timestamp))
        Structs.LastCheckedDF(x._1._1, x._1._2, occurrences.toList)
      }).toDF("eventA", "eventB", "occurrences")
  }

  /**
   * The stored format is [interval_start, interval_end, event_typeA, event_typeB, List of occurrences], where
   * each occurrence has the structure: [trace_id, timestampA, timestampB] if timestamps where used or
   * [trace_id,positionA, positionB] if positions where used.
   * In the second case we save space by just keeping the position of the events in the trace rather than the exact
   * timestamp (replace 20 characters with a single integer).
   * @param df The loaded inverted index using event type pairs
   * @param metaData Object of metadata
   * @return The transformed RDD
   */
  def transformIndexToRDD(df: DataFrame, metaData: MetaData): RDD[Structs.PairFull] = {
    if (metaData.mode == "positions") {
      df.rdd.flatMap(row => {
        val start = row.getAs[Timestamp]("start")
        val end = row.getAs[Timestamp]("end")
        val eventA = row.getAs[String]("eventA")
        val eventB = row.getAs[String]("eventB")
        row.getAs[Seq[Row]]("occurrences").flatMap(oc => {
          val id = oc.getLong(0)
          oc.getAs[Seq[Row]](1).map(o=>{
            Structs.PairFull(eventA, eventB, id, null, null, o.getInt(0), o.getInt(1), Structs.Interval(start,end))
          })

        })
      })
    } else {
      df.rdd.flatMap(row => {
        val start = row.getAs[Timestamp]("start")
        val end = row.getAs[Timestamp]("end")
        val eventA = row.getAs[String]("eventA")
        val eventB = row.getAs[String]("eventB")
        row.getAs[Seq[Row]]("occurrences").flatMap(oc => {
          val id = oc.getLong(0)
          oc.getAs[Seq[Row]](1).map(o => {
            Structs.PairFull(eventA, eventB, id, Timestamp.valueOf(o.getString(0)), Timestamp.valueOf(o.getString(1)),-1,-1, Structs.Interval(start, end))
          })

        })
      })
    }
  }

  /**
   * The loaded format is is [interval_start, interval_end, event_typeA, event_typeB, List of occurrences], where
   * each occurrence has the structure: [trace_id, timestampA, timestampB] if timestamps where used or
   * [trace_id,positionA, positionB] if positions where used.
   * In the second case we save space by just keeping the position of the events in the trace rather than the exact
   * timestamp (replace 20 characters with a single integer).
   *
   * @param pairs The RDD of the computed inverted index based on the event type pairs
   * @param metaData Object of metadata
   * @return The transformed Dataframe
   */
  def transformIndexToDF(pairs: RDD[Structs.PairFull], metaData: MetaData): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    if (metaData.mode == "positions") {
      pairs.groupBy(a => (a.interval, a.eventA, a.eventB))
        .map(b => {
          val occs: List[(Long, List[(Int, Int)])] = b._2.groupBy(_.id)
            .map(c => {
              (c._1, c._2.map(d => (d.positionA, d.positionB)).toList)
            }).toList
          (b._1._1, b._1._2, b._1._3, occs)
        })
        .toDF("interval", "eventA", "eventB", "occurrences")
    } else { //timestamps instead of positions
      pairs.groupBy(a => (a.interval, a.eventA, a.eventB))
        .map(b => {
          val occs: List[(Long, List[(String, String)])] = b._2.groupBy(_.id)
            .map(c => {
              (c._1, c._2.map(d => (d.timeA.toString, d.timeB.toString)).toList)
            }).toList
          (b._1._1, b._1._2, b._1._3, occs)
        })
        .toDF("interval", "eventA", "eventB", "occurrences")
    }
  }

  /**
   * The required format is [event_typeA, List of statistics], where each statistic has the structure:
   * [event_typeB, total_duration, number_of_completions, min_duration, max_duration]. That is
   * for each event type pair, we maintain the following statistics:
   *  - Total duration: sum of all the timestamp differences for each occurrence of this event type
   *  - Total completions: the total number of appearances of this event in the log database
   *  - Min duration: the minimum difference between the two timestamps of the events for this event type pair
   *  - Max duration: the maximum difference between the two timestamps of the events for this event type pair
   * @param counts The RDD containing the statistics for each event type pair
   * @return The transformed Dataframe
   */
  def transformCountToDF(counts:RDD[Structs.Count]):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    counts.groupBy(_.eventA)
      .map(x=>{
        val counts = x._2.map(y=>(y.eventB,y.sum_duration,y.count,y.min_duration,y.max_duration))
        Structs.CountList(x._1,counts.toList)
      })
      .toDF("eventA","times")
  }

  /**
   * The stored format is [event_typeA, List of statistics], where each statistic has the structure:
   * [event_typeB, total_duration, number_of_completions, min_duration, max_duration]. That is
   * for each event type pair, we maintain the following statistics:
   *  - Total duration: sum of all the timestamp differences for each occurrence of this event type
   *  - Total completions: the total number of appearances of this event in the log database
   *  - Min duration: the minimum difference between the two timestamps of the events for this event type pair
   *  - Max duration: the maximum difference between the two timestamps of the events for this event type pair
   *
   * @param df The loaded Dataframe of the CountTable
   * @return The transformed RDD
   */
  def transformCountToRDD(df:DataFrame):RDD[Structs.Count]={
    df.rdd.flatMap(row=>{
      val eventA=row.getAs[String]("eventA")
      row.getAs[Seq[Row]]("times").map(t=>{
        Structs.Count(eventA,t.getString(0),t.getLong(1),t.getInt(2),t.getLong(3),t.getLong(4))
      })
    })
  }


}
