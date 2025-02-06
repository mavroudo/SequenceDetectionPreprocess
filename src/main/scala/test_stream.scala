import auth.datalab.siesta.Utils.Utilities
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext


object test_stream {
  def main(args: Array[String]): Unit = {

    //create spark
    lazy val spark = SparkSession.builder()
      .appName("SIESTA indexing")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val s3accessKeyAws = Utilities.readEnvVariable("s3accessKeyAws")
    val s3secretKeyAws = Utilities.readEnvVariable("s3secretKeyAws")
    val s3ConnectionTimeout = Utilities.readEnvVariable("s3ConnectionTimeout")
    val s3endPointLoc: String = Utilities.readEnvVariable("s3endPointLoc")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.create.enabled", "true")


    spark.sparkContext.setLogLevel("WARN")

    // in order to run it you should first open a server at the port 9999 (can be done by running nc -lk 9999)
//    val socketDF =spark.readStream
//      .format("socket")
//      .option("host","localhost")
//      .option("port","9999")
//      .load
//
//    import spark.implicits._
//    val words = socketDF.as[String]
//      .flatMap(_.split(" "))
//    val pairs = words
//      .map(word => (word, 1))
//      .groupByKey(x=>x._1)
//      .reduceGroups((a,b)=>(a._1,a._2+b._2))
//
//
//    // Write the output to the console
//    val query = pairs.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    // Await termination
//    query.awaitTermination()

  }

}
