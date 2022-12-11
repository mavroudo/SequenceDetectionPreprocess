package auth.datalab.siesta.CassandraConnector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ApacheCassandraTransformations {

  def transformMetaToDF(metadata: MetaData): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(metadata.getClass.getDeclaredFields.foldLeft(Map.empty[String, String]) { (a, f) => {
      f.setAccessible(true)
      a + (f.getName -> f.get(metadata).toString)
    }
    }.toSeq).toDF("key", "value")
  }

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

  case class CassandraSequence(events: List[String], sequence_id: Long)

  def transformSeqToWrite(data: RDD[Structs.Sequence]): RDD[CassandraSequence] = {
    data.map(s => {
      val events = s.events.map(e => s"${e.timestamp},${e.event}")
      CassandraSequence(events, s.sequence_id)
    })
  }

  case class CassandraSingle(event_type: String, occurrences: List[String])

  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
    df.rdd.flatMap(row => {
      val event_name = row.getAs[String]("event_type")
      row.getAs[Seq[String]]("occurrences").map(oc => {
        val s = oc.split("\\|\\|")
        Structs.InvertedSingleFull(s.head.toLong, event_name, s(1).split(",").toList, s(2).split(",").map(_.toInt).toList)
      })
    })
  }

  def transformSingleToWrite(singleRDD: RDD[Structs.InvertedSingleFull]): RDD[CassandraSingle] = {
    singleRDD.groupBy(_.event_name)
      .map(x => {
        val occurrences: List[String] = x._2.map(y => {
          s"${y.id}||${y.times.mkString(",")}||${y.positions.mkString(",")}"
        }).toList
        CassandraSingle(x._1, occurrences)
      })
  }

  case class CassandraLastChecked(event_a:String, event_b:String, occurrences:List[String])

  def transformLastCheckedToRDD(df:DataFrame):RDD[Structs.LastChecked]={
    df.rdd.flatMap(r=>{
      val eventA = r.getAs[String]("event_a")
      val eventB = r.getAs[String]("event_b")
      r.getAs[Seq[String]]("occurrences").map(oc=>{
        val split = oc.split(",")
        Structs.LastChecked(eventA,eventB,split(0).toLong,split(1))
      })
    })
  }

  def transformLastCheckedToWrite(lastChecked: RDD[Structs.LastChecked]):RDD[CassandraLastChecked]={
    lastChecked.groupBy(x=>(x.eventA,x.eventB))
      .map(x=>{
        val occurrences = x._2.map(y=> s"${y.id},${y.timestamp}")
        CassandraLastChecked(x._1._1,x._1._2,occurrences.toList)
      })
  }


}
