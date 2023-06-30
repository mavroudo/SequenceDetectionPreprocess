package auth.datalab.siesta.CassandraConnector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 * This class is responsible to transform the loaded data from Cassandra into RDDs to processed by the main pipeline.
 * Then transform the RDDs to RDDs with specific types, in order to utilize the spark api to store them back in Cassandra.
 * Therefore there are 2 transformations for each table, one to load data and one to write them back.
 */
object ApacheCassandraTransformations {

  /**
   * The required format is (key) -> value, where key is the name of the property and value is its value
   *
   * @param metadata Object containing the metadata
   * @return The transformed Dataframe
   */
  def transformMetaToDF(metadata: MetaData): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(metadata.getClass.getDeclaredFields.foldLeft(Map.empty[String, String]) { (a, f) => {
      f.setAccessible(true)
      a + (f.getName -> f.get(metadata).toString)
    }
    }.toSeq).toDF("key", "value")
  }

  /**
   * The stored format is [sequence_id, List of events], where each event is [event_type, timestamp]
   *
   * @param df The loaded traces from Cassandra
   * @return The transformed RDD of traces
   */
  def transformSeqToRDD(df: DataFrame): RDD[Structs.Sequence] = {
    df.rdd.map(x => {
      val pEvents = x.getAs[Seq[String]]("events").map(e => {
        val splitted = e.split(",")
        Structs.Event(splitted(0), splitted(1))
      })
      val sequence_id = x.getAs[String]("sequence_id").toLong
      Structs.Sequence(pEvents.toList, sequence_id)
    })
  }

  /**
   * This class holds the structure of the SequenceTable. Data in this format can be written in Cassandra
   * using the spark api. That is because this format is the same as the structure of the Sequence Table
   * that is defined in [[CassandraTables]].
   *
   * @param events      The list of events as strings "event_type,timestamp"
   * @param sequence_id The trace id
   */
  case class CassandraSequence(events: List[String], sequence_id: Long)

  /**
   * Transform traces from Structs.Sequence to the specific types CassandraSequence
   *
   * @param data The RDD containing traces in the Sequence format
   * @return The RDD containing the traces in Cassandra compatible format
   */
  def transformSeqToWrite(data: RDD[Structs.Sequence]): RDD[CassandraSequence] = {
    data.map(s => {
      val events = s.events.map(e => s"${e.timestamp},${e.event}")
      CassandraSequence(events, s.sequence_id)
    })
  }

  /**
   * This class holds the structure of the SingleTable. Data in this format can be written in Cassandra
   * using the spark api. That is because this format is the same as the structure of the Single Table
   * that is defined in [[CassandraTables]].
   *
   * @param event_type  The event_type
   * @param trace_id    The id of the trace
   * @param occurrences The list of occurrences of this event type in this particular trace
   */
  case class CassandraSingle(event_type: String, trace_id: Long, occurrences: List[String])

  /**
   * The stored format is (event_type)->[trace,,id,, , List of occurrences], where each occurrence has the structure:
   * "position,timestamp". This represent the position and timestamp of each occurrence of the event_type in the trace
   *
   * @param df The loaded single inverted index
   * @return The transformed RDD
   */
  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
    df.rdd.flatMap(row => {
      val event_name = row.getAs[String]("event_type")
      val trace_id = row.getAs[Long]("trace_id")
      row.getAs[Seq[String]]("occurrences").map(oc => {
        val s = oc.split(",")
        (event_name, trace_id, s(0).toInt, s(1))
      })
        .groupBy(x => (x._1, x._2))
        .map(x => {
          val times = x._2.map(_._4).toList
          val pos = x._2.map(_._3).toList
          Structs.InvertedSingleFull(x._1._2, x._1._1, times, pos)
        })
    })
  }

  /**
   * Transforms the single inverted index into CassandraSingle
   *
   * @param singleRDD The RDD with the single inverted index
   * @return The transformed RDD into Cassandra compatible format
   */
  def transformSingleToWrite(singleRDD: RDD[Structs.InvertedSingleFull]): RDD[CassandraSingle] = {
    singleRDD
      .groupBy(x => (x.event_name, x.id))
      .map(x => {
        val occurrences: List[String] = x._2.flatMap(y => {
          y.positions.zip(y.times).map(a => s"${a._1},${a._2}")
        }).toList
        CassandraSingle(x._1._1, x._1._2, occurrences)
      })
  }

  /**
   * This class holds the structure of the LastChecked table. Data in this format can be written in Cassandra
   * using the spark api. That is because this format is the same as the structure of the LastChecked table
   * that is defined in [[CassandraTables]].
   *
   * @param event_a   The first event type
   * @param event_b   The second event type
   * @param trace_id  The id of the trace
   * @param timestamp The last timestamp that these pair of event types occurred in this particular trace
   */
  case class CassandraLastChecked(event_a: String, event_b: String, trace_id: Long, timestamp: String)

  /**
   * The stored format is [event_typeA, event_typeB, trace_id, timestamp]
   * @param df The loaded data from the LastChecked table
   * @return The transformed RDD
   */
  def transformLastCheckedToRDD(df: DataFrame): RDD[Structs.LastChecked] = {
    df.rdd.map(r => {
      val eventA = r.getAs[String]("event_a")
      val eventB = r.getAs[String]("event_b")
      val trace_id = r.getAs[Long]("trace_id")
      val timestamp = r.getAs[String]("timestamp")
      Structs.LastChecked(eventA, eventB, trace_id, timestamp)
    })
  }

  /**
   * Transforms the LastChecked rdd into CassandraLastChecked
   * @param lastChecked The calculated LastChecked RDD
   * @return The transformed RDD into Cassandra compatible format
   */
  def transformLastCheckedToWrite(lastChecked: RDD[Structs.LastChecked]): RDD[CassandraLastChecked] = {
    lastChecked.map(x => {
      CassandraLastChecked(x.eventA, x.eventB, x.id, x.timestamp)
    })
  }

  /**
   * This class holds the structure of the IndexTable. Data in this format can be written in Cassandra
   * using the spark api. That is because this format is the same as the structure of the IndexTable table
   * that is defined in [[CassandraTables]].
   *
   * @param event_a The first event type
   * @param event_b The second event time
   * @param start The start of the interval
   * @param end The end of the interval
   * @param occurrences The list of all the occurrences of this event type pair, grouped for each different
   *                    trace
   */
  case class CassandraIndex(event_a: String, event_b: String, start: Timestamp, end: Timestamp, occurrences: List[String])

  /**
   * The stored format is (event_typeA, event_typeB)(interval_start, interval_end)-> List of occurrences, where each
   * occurrence is a string " trace,,id,, || timestampA,,1,,|timestampB,,2,, , timestampA,,2,,|timestampB,,2,, "
   * if mode = "timestamps". If mode = "positions" the positions of the events would be stored instead.
   *
   * @param df The loaded data from Cassandra
   * @param metaData Object containing the metadata
   * @return The transformed rdd
   */
  def transformIndexToRDD(df: DataFrame, metaData: MetaData): RDD[Structs.PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    val bc: Broadcast[String] = spark.sparkContext.broadcast(metaData.mode)
    df.rdd.flatMap(r => {
      val eventA = r.getAs[String]("event_a")
      val eventB = r.getAs[String]("event_b")
      val interval = Structs.Interval(r.getAs[Timestamp]("start"), r.getAs[Timestamp]("end"))
      r.getAs[Seq[String]]("occurrences").flatMap(occs => {
        val s_1 = occs.split("\\|\\|")
        val id = s_1(0).toLong
        s_1(1).split(",").map(o => {
          if (bc.value == "positions") {
            val s_2 = o.split("\\|")
            Structs.PairFull(eventA = eventA, eventB = eventB, id = id, timeA = null,
              timeB = null, positionA = s_2(0).toInt, positionB = s_2(1).toInt, interval = interval)
          } else {
            val s_2 = o.split("\\|")
            Structs.PairFull(eventA = eventA, eventB = eventB, id = id, timeA = Timestamp.valueOf(s_2(0)),
              timeB = Timestamp.valueOf(s_2(1)), positionA = -1, positionB = -1, interval = interval)
          }
        })
      })
    })
  }

  /**
   * Transforms the RDD containing the IndexTable into CassandraIndex
   *
   * @param pairs The RDD with the inverted index based on the event type pairs
   * @param metaData Object containing the metadata
   * @return The transformed RDD in Cassandra compatible format
   */
  def transformIndexToWrite(pairs: RDD[Structs.PairFull], metaData: MetaData): RDD[CassandraIndex] = {
    val spark = SparkSession.builder().getOrCreate()
    val bc: Broadcast[String] = spark.sparkContext.broadcast(metaData.mode)
    pairs.groupBy(a => (a.interval, a.eventA, a.eventB))
      .map(b => {
        val occs: List[String] = b._2.groupBy(_.id)
          .map(c => {
            val o: String = c._2.map(d => {
              if (bc.value == "positions") {
                s"${d.positionA}|${d.positionB}" // single | to separate positions
              } else {
                s"${d.timeA}|${d.timeB}" //single | to separate timestamps
              }
            }).mkString(",") //comma to separate the different occurrences corresponding to the same id
            s"${c._1}||$o" // double || to separate id from the occurrences
          }).toList
        CassandraIndex(b._1._2, b._1._3, b._1._1.start, b._1._1.end, occs)
      })
  }

  /**
   * This class holds the structure of the CountTable. Data in this format can be written in Cassandra
   * using the spark api. That is because this format is the same as the structure of the CountTable table
   * that is defined in [[CassandraTables]].
   *
   * @param event_a The first event type
   * @param times The list of all the second event types along with the corresponding statistics
   */
  case class CassandraCount(event_a: String, times: List[String])

  /**
   * Transform the RDD that contains the statistics for each event type pair to CassandraCount objects
   * @param counts The calculated statistics for each event type pair
   * @return The transformed RDD in Cassandra compatible format
   */
  def transformCountToWrite(counts: RDD[Structs.Count]): RDD[CassandraCount] = {
    counts.groupBy(_.eventA)
      .map(x => {
        val times = x._2.map(t => s"${t.eventB},${t.sum_duration},${t.count},${t.max_duration},${t.max_duration}")
        CassandraCount(x._1, times.toList)
      })
  }

  /**
   * The stored format is (event_typeA) -> List of statistics, each statistic has the structure:
   * [event_typeB, total_duration, total_completions, min_duration, max_duration].
   *
   * That is for each event type pair the four above statistics are maintained and updated. The
   * stats are grouped based on the first event type.
   * @param df The loaded data from CountTable
   * @return The transformed RDD
   */
  def transformCountToRDD(df: DataFrame): RDD[Structs.Count] = {
    df.rdd.flatMap(r => {
      val eventA = r.getAs[String]("event_a")
      r.getAs[Seq[String]]("times").map(t => {
        val s = t.split(",")
        Structs.Count(eventA, s(0), s(1).toLong, s(2).toInt, s(3).toLong, s(4).toLong)
      })
    })
  }


}
