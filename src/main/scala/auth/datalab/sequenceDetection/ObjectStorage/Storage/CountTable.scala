package auth.datalab.sequenceDetection.ObjectStorage.Storage

import auth.datalab.sequenceDetection.ObjectStorage.Occurrence
import auth.datalab.sequenceDetection.Structs.CountList
import auth.datalab.sequenceDetection.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.net.URI

object CountTable {
  private var firstTime = true

  def writeTable(combinations:RDD[((String, String), Iterable[Occurrence])],log_name:String,
                 overwrite: Boolean, join: Boolean, splitted_dataset: Boolean): Unit ={
    Logger.getLogger("count_table").log(Level.INFO,"Start writing count table...")
    val count_table: String = s"""s3a://siesta/$log_name/count/"""
    val spark = SparkSession.builder().getOrCreate()
    if (overwrite && firstTime) {
      val fs =FileSystem.get(new URI("s3a://siesta/"),spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(count_table), true)
      firstTime=false
    }
    val countList= flattener(combinations)
    val df = {
      if(splitted_dataset || join){
        this.combine(countList,count_table)
      }else{
        this.transformToDF(countList)
      }
    }
    val mode = if(!splitted_dataset && !join) SaveMode.ErrorIfExists else SaveMode.Overwrite
    this.simpleWrite(df,mode,count_table)
  }

  private def simpleWrite(df:DataFrame,mode:SaveMode,count_table:String):Unit ={
    try {
      df
        .repartition(col("event"))
        .write
        .partitionBy("event")
        .mode(mode)
        .parquet(count_table)
    }catch {
      case _: org.apache.spark.sql.AnalysisException =>
        Logger.getLogger("count_table").log(Level.WARN,"Couldn't find table, so simply writing it")
        val mode2 = if (mode==SaveMode.Overwrite) SaveMode.ErrorIfExists else SaveMode.Overwrite
        df
          .repartition(col("event"))
          .write
          .partitionBy("event")
          .mode(mode2)
          .parquet(count_table)
    }
  }

  private def transformToDF(countList:RDD[(String, String, Long, Int)]):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    countList
      .groupBy(_._1)
      .map(x => {
        val l = x._2.map(t => (t._2, t._3, t._4))
        CountList(x._1, l.toList)
      }).toDF("event", "times")
  }

  private def combine(countList:RDD[(String, String, Long, Int)],count_table:String):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    try {
      val prev: RDD[((String, String), Long, Int)] = spark.read.parquet(count_table)
        .flatMap(row => {
          val eventA: String = row.getAs[String]("event")
          row.getAs[Seq[Row]]("times").map(y => {
            ((eventA, y.getString(0)), y.getLong(1), y.getInt(2))
          })
        }).rdd
      countList
        .map(x => ((x._1, x._2), x._3, x._4))
        .keyBy(_._1)
        .fullOuterJoin(prev.keyBy(_._1))
        .map(x => {
          val t1 = x._2._1.getOrElse(("", 0L, 0))._3
          val t2 = x._2._2.getOrElse(("", 0L, 0))._3
          val d1 = x._2._1.getOrElse(("", 0L, 0))._2
          val d2 = x._2._2.getOrElse(("", 0L, 0))._2
          val average_duration = (d1 * t1 + d2 * t2) / (t1 + t2)
          (x._1._1, x._1._2, average_duration, t1 + t2)
        })
        .keyBy(_._1)
        .groupByKey()
        .map(x => {
          val l = x._2.map(t => (t._2, t._3, t._4))
          CountList(x._1, l.toList)
        }).toDF("event", "times")
    }catch{
      case _ @ (_:java.lang.NullPointerException | _:org.apache.spark.sql.AnalysisException) =>
        this.transformToDF(countList)
    }
  }


  private def flattener(combinations:RDD[((String, String), Iterable[Occurrence])]):RDD[(String, String, Long, Int)]={
    combinations.map(x => {
      var l = 0L
      var t = 0
      x._2.foreach(oc => {
        t += 1
        l += Utils.getDifferenceTime(oc.tA, oc.tB)
      })
      (x._1._1, x._1._2, l / t, t)
    })
  }

}
