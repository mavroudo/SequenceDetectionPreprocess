package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp

object S3Transformations {

  def transformSeqToDF(sequenceRDD: RDD[Structs.Sequence]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    sequenceRDD.map(x => {
      (x.sequence_id, x.events.map(y => (y.event, y.timestamp)))
    }).toDF("trace_id", "events")
  }

  def transformSeqToRDD(df: DataFrame): RDD[Structs.Sequence] = {
    df.rdd.map(x => {
      val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
      val concat = pEvents.map(x => Structs.Event(x._2, x._1))
      Structs.Sequence(concat.toList, x.getAs[Long]("trace_id"))
    })
  }

  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
    df.rdd.flatMap(x => {
      val event_name = x.getAs[String]("event_type")
      val occurrences = x.getAs[Seq[Row]]("occurrences").map(y => (y.getLong(0), y.getAs[Seq[String]](1), y.getAs[Seq[Int]](2)))
      occurrences.map(o => {
        Structs.InvertedSingleFull(o._1, event_name, o._2.toList, o._3.toList)
      })
    })
  }

  def transformSingleToDF(singleRDD: RDD[Structs.InvertedSingleFull]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    singleRDD.groupBy(_.event_name)
      .map(y => {
        Structs.InvertedSingle(y._1, y._2.map(x => Structs.IdTimePositionList(x.id, x.times, x.positions)).toList)

      }).toDF("event_type", "occurrences")
  }

  def transformLastCheckedToRDD(df: DataFrame): RDD[LastChecked] = {
    df.rdd.flatMap(x => {
      val eventA = x.getAs[String]("eventA")
      val eventB = x.getAs[String]("eventB")
      x.getAs[Seq[Row]]("occurrences").map(oc => {
        LastChecked(eventA, eventB, oc.getLong(0), oc.getString(1))
      })
    })
  }

  def transformLastCheckedToDF(lastchecked: RDD[LastChecked]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    lastchecked.groupBy(x => (x.eventA, x.eventB))
      .map(x => {
        val occurrences = x._2.map(y => Structs.IdTime(y.id, y.timestamp))
        Structs.LastCheckedDF(x._1._1, x._1._2, occurrences.toList)
      }).toDF("eventA", "eventB", "occurrences")
  }

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

  def transformCountToRDD(df:DataFrame):RDD[Structs.Count]={
    df.rdd.flatMap(row=>{
      val eventA=row.getAs[String]("eventA")
      row.getAs[Seq[Row]]("times").map(t=>{
        Structs.Count(eventA,t.getString(0),t.getLong(1),t.getInt(2),t.getLong(3),t.getLong(4))
      })
    })
  }


}
