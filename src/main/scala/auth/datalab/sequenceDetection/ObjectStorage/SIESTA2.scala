package auth.datalab.sequenceDetection.ObjectStorage

import auth.datalab.sequenceDetection.CommandLineParser.Utilities.Iterations
import auth.datalab.sequenceDetection.CommandLineParser.{Config, Utilities}
import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SIESTA2 {

  def execute(c: Config): Unit = {
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


//    val df = spark.read.parquet(s"""s3a://siesta/log-100-113/idx/""")

    val init = Utilities.getRDD(c, 100)
    val sequencesRDD_before_repartitioned = init.data
    val traces: Int = Utilities.getTraces(c, sequencesRDD_before_repartitioned)
    val iterations: Iterations = Utilities.getIterations(c, sequencesRDD_before_repartitioned, traces)
    val ids: List[Array[Long]] = Utilities.getIds(c, sequencesRDD_before_repartitioned, traces, iterations.iterations)
    val table_name = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0)
      .replace(' ', '-')
      .replace('_', '-')

    var k = 0L
    for (id <- ids) {
      val sequencesRDD: RDD[Structs.Sequence]=Utilities.getNextData(c,sequencesRDD_before_repartitioned,id,init.traceGenerator,iterations.allExecutors)
      k += Preprocess.execute(sequencesRDD, table_name,c.delete_previous,c.join,ids.size>1)
    }

    println(s"Time taken: $k ms")
    spark.close()


    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("ALL RESULTS IN MB")
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)

  }

}
