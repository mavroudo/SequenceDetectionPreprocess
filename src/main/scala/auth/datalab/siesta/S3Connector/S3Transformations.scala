package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.LastChecked
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

  def transformSingleToRDD(df:DataFrame):RDD[Structs.InvertedSingleFull]={
    df.rdd.flatMap(x=>{
      val event_name = x.getAs[String]("event_type")
      val occurrences = x.getAs[Seq[Row]]("occurrences").map(y=>(y.getLong(0),y.getAs[Seq[String]](1)))
      occurrences.map(o=>{
        Structs.InvertedSingleFull(o._1,event_name,o._2.toList)
      })
    })
  }

  def transformSingleToDF(singleRDD:RDD[Structs.InvertedSingleFull]): DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    singleRDD.groupBy(_.event_name)
      .map(y=>{
        Structs.InvertedSingle(y._1,y._2.map(x=>Structs.IdTimeList(x.id,x.times)).toList)
      }).toDF("event_type", "occurrences")
  }

  def transformLastCheckedToRDD(df: DataFrame):RDD[LastChecked]={
    df.rdd.flatMap(x=>{
      val events = x.getAs[Seq[String]]("key_events").toList
      x.getAs[Seq[Row]]("occurrences").map(oc=>{
        LastChecked(events.head,events,oc.getLong(0),oc.getString(1))
      })
    })
  }

  def transformLastCheckedToDF(lastchecked:RDD[LastChecked]):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    lastchecked.groupBy(x=>x.events)
      .map(x=>{
        val occurrences = x._2.map(y=>Structs.IdTime(y.id,y.timestamp))
        Structs.LastCheckedDF(x._1.head,x._1,occurrences.toList)
      }).toDF("first_event","key_events","occurrences")
  }





}
