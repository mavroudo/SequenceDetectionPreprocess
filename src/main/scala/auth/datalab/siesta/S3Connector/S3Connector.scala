package auth.datalab.siesta.S3Connector

import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.sql.SparkSession

class S3Connector extends DBConnector{
  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  override def initialize_spark(): Unit = {
    lazy val spark = SparkSession.builder()
      .appName("Object Storage Test")
      .master("local[*]")
      .getOrCreate()

    //TODO: pass through environment vars
    val s3accessKeyAws = "minioadmin"
    val s3secretKeyAws = "minioadmin"
    val connectionTimeOut = "600000"
    val s3endPointLoc: String = "http://127.0.0.1:9000"

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", connectionTimeOut)
    //    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  }

  /**
   * This method should construct the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   *
   * @param config contains the configuration passed during execution
   * @return the metadata
   */
  override def get_metadata(config: Config): MetaData = {

    null
  }
}
