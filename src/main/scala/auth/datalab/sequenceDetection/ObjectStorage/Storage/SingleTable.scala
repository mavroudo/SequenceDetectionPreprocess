package auth.datalab.sequenceDetection.ObjectStorage.Storage

import auth.datalab.sequenceDetection.Structs.{IdTimeList, InvertedOne, Sequence}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger, Priority}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.net.URI




object SingleTable {
  private var firstTime=true

  def writeTable(newSequences: RDD[Sequence], log_name: String, overwrite: Boolean, join: Boolean, split_dataset: Boolean): Unit = {
    Logger.getLogger("single_table").log(Level.INFO,"Start writing single table...")
    val single_table: String = s"""s3a://siesta/$log_name/single/"""
    val spark = SparkSession.builder().getOrCreate()
    val inverted = this.calculateSingle(newSequences)
    if (overwrite && firstTime) {
      val fs =FileSystem.get(new URI("s3a://siesta/"),spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(single_table), true)
      firstTime=false
    }
    val mode = SaveMode.ErrorIfExists
    if (!join && !split_dataset) { //simple write based on the mode
      this.simpleWrite(inverted, mode, single_table)
    } else { // combine with previous if exist
      this.combineWrite(inverted,single_table)
    }
  }

  def calculateSingle(newSequences: RDD[Sequence]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    newSequences.flatMap(x => {
      x.events.map(event => {
        ((event.event, x.sequence_id), event.timestamp)
      })
    }).groupBy(_._1)
      .map(y => {
        val times = y._2.map(_._2)
        (y._1._1, y._1._2, times)
      })
      .groupBy(_._1)
      .map(y => {
        val times = y._2.map(x => IdTimeList(x._2, x._3.toList))
        InvertedOne(y._1, times.toList)
      }).toDF("event_type", "occurrences")
  }

  private def simpleWrite(df: DataFrame, mode: SaveMode, single_table: String): Unit = {
    try {
      df
        .repartition(col("event_type"))
        .write
        .partitionBy("event_type")
        .mode(mode)
        .parquet(single_table)
    }catch {
      case _: org.apache.spark.sql.AnalysisException =>
        Logger.getLogger("single_table").log(Level.WARN,"Couldn't find table, so simply writing it")
        val mode2 = if (mode==SaveMode.Overwrite) SaveMode.ErrorIfExists else SaveMode.Overwrite
        df
          .repartition(col("event_type"))
          .write
          .partitionBy("event_type")
          .mode(mode2)
          .parquet(single_table)
    }
  }

  private def combineWrite(df: DataFrame, single_table: String): Unit = {
    try {
      val newdf = this.combineWithPrev(df, single_table)
      this.simpleWrite(newdf,SaveMode.Overwrite,single_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        this.simpleWrite(df, SaveMode.ErrorIfExists, single_table)
    }

  }

  private def combineWithPrev(df: DataFrame,single_table: String):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    try {
      val dfPrev = spark.read.parquet(single_table)
      df.withColumnRenamed("occurrences", "newOccurrences")
        .join(right = dfPrev, usingColumns = Seq("event_type"), joinType = "full")
        .rdd.map(row => {
        val nTimes: Seq[(Long, Seq[String])] = row.getAs[Seq[Row]]("newOccurrences").map(o => {
          (o.getLong(0), o.getAs[Seq[String]](1))
        })
        val pTimes: Seq[(Long, Seq[String])] = row.getAs[Seq[Row]]("occurrences").map(o => {
          (o.getLong(0), o.getAs[Seq[String]](1))
        })
        val combined = Seq.concat(nTimes, pTimes).groupBy(_._1).map(x => {
          (x._1, x._2.flatMap(_._2))
        }).toSeq
        (row.getString(0), combined)
      }).toDF("event_type", "occurrences")
    }catch {
      case _ @ (_:java.lang.NullPointerException | _:org.apache.spark.sql.AnalysisException) =>
        df
    }
  }

}
