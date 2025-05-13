package auth.datalab.siesta.Utils


import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Utilities {

  /**
   * Read environment variable
   *p
   * @param key The key of the variable
   * @return The variable
   * @throws NullPointerException if the variable does not exist
   */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
  }

  def get_activity_matrix(event_types_occurrences:scala.collection.Map[String, Long]):RDD[(String,String)]={
    val spark = SparkSession.builder().getOrCreate()

    val keys: Iterable[String] = event_types_occurrences.keys

    val cartesianProduct: Iterable[(String, String)] = for {
      key1 <- keys
      key2 <- keys
    } yield (key1, key2)

    SparkSession.builder().getOrCreate().sparkContext.parallelize(cartesianProduct.toSeq)

  }



}
