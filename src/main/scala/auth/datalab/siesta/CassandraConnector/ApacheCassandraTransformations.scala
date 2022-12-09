package auth.datalab.siesta.CassandraConnector

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ApacheCassandraTransformations {

  def transformMetaToDF(metadata:MetaData):DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(metadata.getClass.getDeclaredFields.foldLeft(Map.empty[String,String]){(a,f) =>{
      f.setAccessible(true)
      a+(f.getName -> f.get(metadata).toString)
    }}.toSeq).toDF("key","value")
  }

  def transformSeqToRDD(df: DataFrame): RDD[Structs.Sequence] = {
    df.rdd.map(x => {
      val pEvents = x.getAs[Seq[Structs.Event]]("events")
      val sequence_id = x.getAs[String]("sequence_id").toLong
      Structs.Sequence(pEvents.toList,sequence_id)
    })
  }




}
