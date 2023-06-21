package auth.datalab.siesta.S3ConnectorStreaming

import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

class S3ConnectorStreaming {
  var seq_table: String = _
  var meta_table: String = _
  var single_table: String = _
  var last_checked_table: String = _
  var index_table: String = _
  var count_table: String = _

  def get_spark_context(config: Config): SparkSession = {

    val s3accessKeyAws = Utilities.readEnvVariable("s3accessKeyAws")
    val s3secretKeyAws = Utilities.readEnvVariable("s3secretKeyAws")
    val s3ConnectionTimeout = Utilities.readEnvVariable("s3ConnectionTimeout")
    val s3endPointLoc: String = Utilities.readEnvVariable("s3endPointLoc")

    val conf = new SparkConf()
      .setAppName("Siesta incremental")
      .setMaster("local[2]")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.parquet.compression.codec", config.compression)
      .set("spark.sql.parquet.filterPushdown", "true")

    //    val scc = new StreamingContext(conf, Seconds(5)) //TODO: parametrize it
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    //    scc.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")

    spark
  }


}
