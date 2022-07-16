package auth.datalab.sequenceDetection.ObjectStorage


import auth.datalab.sequenceDetection.{Structs, Utils}
import auth.datalab.sequenceDetection.Structs.{CountList, Sequence}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Preprocess {
  case class SequenceP(events: List[EventP], trace_id: Long)

  case class EventP(event_type: String, position: Long, timestamp:String)

  var join:Boolean = false
  var overwrite:Boolean = false

  def execute(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean, join:Boolean): Long = {
    this.overwrite=overwrite
    this.join=join
//    case class PairOccs(eventA: String, eventB: String, l: List[(Long, List[(Long, Long)])])
    val start = System.currentTimeMillis()
    sequenceRDD.persist(StorageLevel.MEMORY_AND_DISK)
    this.writeSeq(sequenceRDD,log_name,overwrite)



//    val combinations=this.extractOccurrences(sequenceRDD)
//      .groupBy(_.getPair)
//    combinations.persist(StorageLevel.MEMORY_AND_DISK)
    sequenceRDD.unpersist()

//    this.writeIndex(combinations,log_name, overwrite)

//    this.createCountTable(combinations,log_name, overwrite)

//    combinations.unpersist()
    System.currentTimeMillis() - start


  }

  private def extractPairs(s: SequenceP): List[Occurrence] = {
    var mapping = mutable.HashMap[String, (List[Long],List[String])]()

    s.events.foreach(event => { //mapping events to positions
      val oldSequence = mapping.getOrElse(event.event_type, null)
      if (oldSequence == null) {
        mapping.+=((event.event_type, (List(event.position),List(event.timestamp))))
      } else {
        val newListPos = oldSequence._1 :+ event.position
        val newListTime = oldSequence._2 :+ event.timestamp
        mapping.+=((event.event_type, (newListPos,newListTime)))
      }
    })
    mapping.flatMap(eventA => { //double for loop in distinct events
      mapping.flatMap(eventB => {
        this.createPairs(eventA._1, eventB._1, eventA._2, eventB._2, s.trace_id)
      })
    }).toList

  }

  private def createPairs(eventA: String, eventB: String, eventAtimes: (List[Long],List[String]), eventBtimes: (List[Long],List[String]), traceid: Long): List[Occurrence] = {
    var posA = 0
    var posB = 0
    var prev = -1L

    val tA = eventAtimes._1.zip(eventAtimes._2).sortWith((x,y)=>x._1<y._1)
    val tB = eventBtimes._1.zip(eventBtimes._2).sortWith((x,y)=>x._1<y._1)


    val response = new ListBuffer[Occurrence]
    while (posA < tA.size && posB < tB.size) {
      if (tA(posA)._1 < tB(posB)._1) { // goes in if a  b
        if (prev < tA(posA)._1) {
          response += new Occurrence(eventA, eventB, traceid, tA(posA)._1, tB(posB)._1,tA(posA)._2,tB(posB)._2)
          prev = tB(posB)._1
          posA += 1
          posB += 1
        } else {
          posA += 1
        }
      } else {
        posB += 1
      }
    }
    response.toList
  }

  private def writeSeq(sequences:RDD[Structs.Sequence],log_name:String):Unit={
    val index_table: String = s"""s3a://siesta/$log_name/seq/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._

    val df = sequences.map(x=>{
      (x.sequence_id,x.events.map(y=>(y.event,y.timestamp)))
    }).toDF("trace_id","events")


    if (this.overwrite) {
      df.sort("trace_id")
        .write
        .mode("overwrite")
        .parquet(index_table)
    } else if(this.join){
      val dfPrev = spark.read.parquet(index_table)
      df.withColumnRenamed("events","newEvents")
        .join(dfPrev,"trace_id")
        .rdd.map(x=>{
        val pEvents = x.getAs[Seq[Row]]("events").map(y=>(y.getString(0),y.getString(1)))
        val nEvents = x.getAs[Seq[Row]]("newEvents").map(y=>(y.getString(0),y.getString(1)))
          (x.getAs[Long]("trace_id"),Seq.concat(pEvents,nEvents))
        }).toDF("trace_id","events")
      .sort("trace_id")
        .write
        .mode("overwrite")
        .parquet(index_table)
    }else{
      df.sort("trace_id")
        .write
        .parquet(index_table)
    }

  }

  private def extractOccurrences(sequences:RDD[Structs.Sequence]):RDD[Occurrence]={
    val dataP = sequences.map(trace => {
      val events = trace.events.zipWithIndex.map(x => {
        EventP(x._1.event, x._2,x._1.timestamp)
      })
      SequenceP(events, trace.sequence_id)
    })

    dataP
      .flatMap(l => this.extractPairs(l))
  }

  private def writeIndex(comb:RDD[((String,String),Iterable[Occurrence])],log_name:String):Unit={
    val index_table: String = s"""s3a://siesta/$log_name/idx/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val combinations = comb
      .map(pair => {
        val occs = pair._2.groupBy(_.trace)
          .map(x => {
            (x._1, x._2.map(y => (y.pA, y.pB)).toList)
          }).toList
        (pair._1._1, pair._1._2, occs)
      })
    val df = combinations.toDF("eventA", "eventB", "occurrences")
    if (this.overwrite) {
      df
        .repartition(col("eventA"))
        .write
        .partitionBy("eventA")
        .mode("overwrite")
        .parquet(index_table)
    } else {
      val dfPrev=spark.read.format("csv").option("header",value = true).load(index_table)
      dfPrev.printSchema()


//      df.repartition(col("eventA"))
//        .write
//        .partitionBy("eventA")
//        .parquet(index_table)
    }

  }

  private def createCountTable(comb:RDD[((String,String),Iterable[Occurrence])],log_name:String,overwrite:Boolean):Unit={
    val countList = comb.map(x=>{
      var l=0L
      var t=0
      x._2.foreach(oc=>{
        t+=1
        l+=Utils.getDifferenceTime(oc.tA,oc.tB)
      })
      (x._1._1,x._1._2,l,t)
    }).groupBy(_._1)
      .map(x=>{
        val l = x._2.map(t=>(t._2,t._3,t._4))
        CountList(x._1,l.toList)
      })
    val index_table: String = s"""s3a://siesta/$log_name/count/"""
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    val df=countList.toDF("event","times")
    if(overwrite){
      df
      .repartition(col("event"))
        .write
        .partitionBy("event")
        .mode("overwrite")
        .parquet(index_table)
    }

  }
}
