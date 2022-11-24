package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.Model.Structs
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



}
