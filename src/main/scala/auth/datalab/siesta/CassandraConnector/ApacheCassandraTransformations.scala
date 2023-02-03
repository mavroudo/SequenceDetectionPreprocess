package auth.datalab.siesta.CassandraConnector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

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

//  case class CassandraSingle(event_type: String, occurrences: List[String])

  case class CassandraSingle(event_type:String, trace_id: Long, occurrences:List[String])

//  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
//    df.rdd.flatMap(row => {
//      val event_name = row.getAs[String]("event_type")
//      row.getAs[Seq[String]]("occurrences").map(oc => {
//        val s = oc.split("\\|\\|")
//        Structs.InvertedSingleFull(s.head.toLong, event_name, s(1).split(",").toList, s(2).split(",").map(_.toInt).toList)
//      })
//    })
//  }

  def transformSingleToRDD(df: DataFrame): RDD[Structs.InvertedSingleFull] = {
    df.rdd.flatMap(row=>{
      val event_name = row.getAs[String]("event_type")
      val trace_id = row.getAs[Long]("trace_id")
      row.getAs[Seq[String]]("occurrences").map(oc => {
        val s = oc.split(",")
        (event_name,trace_id,s(0).toInt,s(1))
      })
        .groupBy(x=>(x._1,x._2))
        .map(x=>{
          val times = x._2.map(_._4).toList
          val pos = x._2.map(_._3).toList
          Structs.InvertedSingleFull(x._1._2,x._1._1,times,pos)
        })
    })
  }

  def transformSingleToWrite(singleRDD: RDD[Structs.InvertedSingleFull]): RDD[CassandraSingle] = {
    singleRDD
      .groupBy(x=>(x.event_name,x.id))
      .map(x=>{
        val occurrences:List[String] = x._2.flatMap(y=>{
          y.positions.zip(y.times).map(a=>s"${a._1},${a._2}")
        }).toList
          CassandraSingle(x._1._1,x._1._2,occurrences)
      })
  }

//  def transformSingleToWrite(singleRDD: RDD[Structs.InvertedSingleFull]): RDD[CassandraSingle] = {
//    singleRDD.groupBy(_.event_name)
//      .map(x => {
//        val occurrences: List[String] = x._2.map(y => {
//          s"${y.id}||${y.times.mkString(",")}||${y.positions.mkString(",")}"
//        }).toList
//        CassandraSingle(x._1, occurrences)
//      })
//  }

//  case class CassandraLastChecked(event_a: String, event_b: String, occurrences: List[String])
  case class CassandraLastChecked(event_a: String, event_b: String, trace_id :Long, timestamp: String)

    def transformLastCheckedToRDD(df: DataFrame): RDD[Structs.LastChecked] = {
      df.rdd.map(r => {
        val eventA = r.getAs[String]("event_a")
        val eventB = r.getAs[String]("event_b")
        val trace_id = r.getAs[Long]("trace_id")
        val timestamp = r.getAs[String]("timestamp")
        Structs.LastChecked(eventA,eventB,trace_id,timestamp)
      })
    }



//  def transformLastCheckedToRDD(df: DataFrame): RDD[Structs.LastChecked] = {
//    df.rdd.flatMap(r => {
//      val eventA = r.getAs[String]("event_a")
//      val eventB = r.getAs[String]("event_b")
//      r.getAs[Seq[String]]("occurrences").map(oc => {
//        val split = oc.split(",")
//        Structs.LastChecked(eventA, eventB, split(0).toLong, split(1))
//      })
//    })
//  }

  def transformLastCheckedToWrite(lastChecked: RDD[Structs.LastChecked]): RDD[CassandraLastChecked] = {
    lastChecked.map(x=>{
      CassandraLastChecked(x.eventA,x.eventB,x.id,x.timestamp)
    })
  }

//  def transformLastCheckedToWrite(lastChecked: RDD[Structs.LastChecked]): RDD[CassandraLastChecked] = {
//    lastChecked.groupBy(x => (x.eventA, x.eventB))
//      .map(x => {
//        val occurrences = x._2.map(y => s"${y.id},${y.timestamp}")
//        CassandraLastChecked(x._1._1, x._1._2, occurrences.toList)
//      })
//  }

  case class CassandraIndex(event_a: String, event_b: String, start: Timestamp, end: Timestamp, occurrences: List[String])

  def transformIndexToRDD(df: DataFrame, metaData: MetaData): RDD[Structs.PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    val bc: Broadcast[String] = spark.sparkContext.broadcast(metaData.mode)
    df.rdd.flatMap(r=>{
      val eventA = r.getAs[String]("event_a")
      val eventB = r.getAs[String]("event_b")
      val interval = Structs.Interval(r.getAs[Timestamp]("start"),r.getAs[Timestamp]("end"))
      r.getAs[Seq[String]]("occurrences").flatMap(occs=>{
        val s_1 = occs.split("\\|\\|")
        val id = s_1(0).toLong
        s_1(1).split(",").map(o=>{
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

  def transformIndexToWrite(pairs: RDD[Structs.PairFull], metaData: MetaData):RDD[CassandraIndex]={
    val spark =SparkSession.builder().getOrCreate()
    val bc:Broadcast[String] = spark.sparkContext.broadcast(metaData.mode)
    pairs.groupBy(a => (a.interval, a.eventA, a.eventB))
      .map(b => {
        val occs: List[String] = b._2.groupBy(_.id)
          .map(c => {
            val o:String = c._2.map(d=>{
              if(bc.value=="positions"){
                s"${d.positionA}|${d.positionB}" // single | to separate positions
              }else{
                s"${d.timeA}|${d.timeB}"  //single | to separate timestamps
              }
            }).mkString(",") //comma to separate the different occurrences corresponding to the same id
            s"${c._1}||$o" // double || to separate id from the occurrences
          }).toList
        CassandraIndex(b._1._2, b._1._3, b._1._1.start, b._1._1.end, occs)
      })
  }

  case class CassandraCount(event_a:String, times:List[String])

  def transformCountToWrite(counts:RDD[Structs.Count]):RDD[CassandraCount]={
    counts.groupBy(_.eventA)
      .map(x=>{
        val times = x._2.map(t=> s"${t.eventB},${t.sum_duration},${t.count},${t.max_duration},${t.max_duration}")
        CassandraCount(x._1,times.toList)
      })
  }

  def transformCountToRDD(df:DataFrame):RDD[Structs.Count]={
    df.rdd.flatMap(r=>{
      val eventA=r.getAs[String]("event_a")
      r.getAs[Seq[String]]("times").map(t => {
        val s = t.split(",")
        Structs.Count(eventA, s(0), s(1).toLong, s(2).toInt,s(3).toLong,s(4).toLong)
      })
    })
  }




}
