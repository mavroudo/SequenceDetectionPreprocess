package auth.datalab.sequenceDetection.ObjectStorage.Storage

import auth.datalab.sequenceDetection.Structs.{Event, Sequence}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.net.URI

object SequenceTable {
  private var firstTime= true
  def writeTable(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean, join: Boolean, splitted_dataset: Boolean): RDD[Sequence] = {
    Logger.getLogger("seq_table").log(Level.INFO,"Start writing seq table...")
    val seq_table: String = s"""s3a://siesta/$log_name/seq/"""
    val spark = SparkSession.builder().getOrCreate()
    if (overwrite && firstTime) {
      val fs =FileSystem.get(new URI("s3a://siesta/"),spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(seq_table), true)
      firstTime=false
    }
    val df: DataFrame = this.getDataFrame(sequenceRDD, seq_table, join) //will combine if needed
    val mode = {
      if (join) SaveMode.Overwrite
      else if (splitted_dataset) SaveMode.Append
      else SaveMode.ErrorIfExists
    }
    this.simpleWrite(df, mode, seq_table)
    if (!join) {
      sequenceRDD
    } else { // in case of join return the combined sequences after removing any old events
      this.filterOldSeq(this.transformToRDD(df))
    }


  }

  private def transformToDF(sequenceRDD: RDD[Sequence]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    sequenceRDD.map(x => {
      (x.sequence_id, x.events.map(y => (y.event, y.timestamp)))
    }).toDF("trace_id", "events")
  }

  private def transformToRDD(df: DataFrame): RDD[Sequence] = {
    df.rdd.map(x => {
      val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
      val concat = pEvents.map(x => Event(x._1, x._2))
      Sequence(concat.toList, x.getAs[Long]("trace_id"))
    })
  }

  private def simpleWrite(df: DataFrame, mode: SaveMode, seq_table: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      df
        .sort("trace_id")
        .repartition(spark.sparkContext.defaultParallelism)
        .write
        .mode(mode)
        .parquet(seq_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        Logger.getLogger("seq_table").log(Level.INFO, "The mode was not applicable, modifying")
        // if file already exists - mode => overwrite. If file doesn't exist - mode => ErrorIfExists
        val mode2 = if (mode == SaveMode.ErrorIfExists) SaveMode.Overwrite else SaveMode.ErrorIfExists
        df
          .sort("trace_id")
          .repartition(spark.sparkContext.defaultParallelism)
          .write
          .mode(mode2)
          .parquet(seq_table)
    }

  }

  private def getDataFrame(sequenceRDD: RDD[Sequence], seq_table: String, join: Boolean): DataFrame = {
    val df = this.transformToDF(sequenceRDD)
    if (!join) {
      df
    } else {
      this.combineSequence(df, seq_table)
    }
  }

  private def combineSequence(df: DataFrame, seq_table: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    try {
      val dfPrev = spark.read.parquet(seq_table)
      df.withColumnRenamed("events", "newEvents")
        .join(dfPrev, "trace_id")
        .rdd.map(x => {
        val pEvents = x.getAs[Seq[Row]]("events").map(y => (y.getString(0), y.getString(1)))
        val nEvents = x.getAs[Seq[Row]]("newEvents").map(y => (y.getString(0), y.getString(1)))
        (x.getAs[Long]("trace_id"), Seq.concat(pEvents, nEvents))
      }).toDF("trace_id", "events")
    }catch {
      case _ @ (_:java.lang.NullPointerException | _:org.apache.spark.sql.AnalysisException) =>
        df
    }
  }

  private def filterOldSeq(sequenceRDD: RDD[Sequence]): RDD[Sequence] = {
    //TODO: implement that with lookback days
    sequenceRDD
  }

}
