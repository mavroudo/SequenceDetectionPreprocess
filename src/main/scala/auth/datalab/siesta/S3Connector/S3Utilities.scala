package auth.datalab.siesta.S3Connector

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI

object S3Utilities {

  def extractSingleTables(singleTable:String,events:List[String]):Seq[String]={
    val spark = SparkSession.builder().getOrCreate()
    val fs = FileSystem.get(new URI(singleTable), spark.sparkContext.hadoopConfiguration)
    val paths = events.filter(x=>fs.exists(new Path(singleTable+"event_type="+x)))
    paths
  }

}
