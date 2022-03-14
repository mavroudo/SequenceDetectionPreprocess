package auth.datalab.sequenceDetection.Triplets

import auth.datalab.sequenceDetection.{CassandraConnectionTrait, Structs}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class CassandraTriplets extends Serializable with CassandraConnectionTrait{

  def createTable(logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_temp = logName + "_trip_temp"
    val table_seq = logName + "_trip_seq"
    val table_idx = logName + "_trip_idx"
    val table_count = logName + "_trip_count"
    val tables: Map[String, String] = Map(
      table_idx -> "event1_name text, event2_name text, event3_name text, sequences list<text>, PRIMARY KEY (event1_name, event2_name, event3_name)",
      table_temp -> "event1_name text, event2_name text, event3_name text,  sequences list<text>, PRIMARY KEY (event1_name, event2_name, event3_name)",
      table_count -> "event1_name text, event2_name text, sequences_per_field list<text>, PRIMARY KEY (event1_name,event2_name)",
      table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)"
    )
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- tables) {
          session.execute("CREATE TABLE IF NOT EXISTS " + cassandra_keyspace_name + "." +
            table._1 + " (" + table._2 + ") " +
            "WITH GC_GRACE_SECONDS=" + cassandra_gc_grace_seconds +
            ";")
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.out.println("A problem occurred creating the table")
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  def dropTable(logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_temp = logName + "_trip_temp"
    val table_seq = logName + "_trip_seq"
    val table_idx = logName + "_trip_idx"
    val table_count = logName + "_trip_count"
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_temp + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_seq + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_idx + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_count + ";")
      }
    }
    catch {
      case e: Exception =>
        System.out.println("A problem occurred dropping the tables")
        e.printStackTrace()
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit ={
    val table_seq = logName + "_trip_seq"
    table.filter(_.events.nonEmpty).saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = table_seq.toLowerCase(),columns = SomeColumns("events","sequence_id"), writeConf = writeConf)
  }

  def writeTableSequenceIndex(combinations: RDD[Structs.Triplet], logName: String): Unit = {
    val table_idx = logName + "_trip_idx"
    val spark = SparkSession.builder().getOrCreate()
    val table = combinations
      .map(r => {
        val formatted = combinationsToCassandraFormat(r)
        Structs.CassandraIndexTriplets(formatted._1, formatted._2, formatted._3, formatted._4)
      })
    //    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = table_idx.toLowerCase,
      columns = SomeColumns(
        "event1_name",
        "event2_name",
        "event3_name",
        "sequences" append //Method to append to a list in cassandra
      ), writeConf
    )
  }

  private def combinationsToCassandraFormat(line: Structs.Triplet): (String, String, String, List[String]) = {
    val newList = line.times
      .map(r => {
        var userString = r.id + "("
        for (i <- 2 until r.times.length by 3) {
          userString = userString + "(" + r.times(i - 2) + "," + r.times(i-1) + "," + r.times(i)+ "),"
        }
        userString = userString.dropRight(1) + ")"
        userString
      })
    (line.event1, line.event2,line.event3, newList)
  }

//  private def combinationsCountToCassandraFormat(line: Structs.TripleCountList): (String, String, List[String]) = {
//    val newEvent = line.event1_name
//    val newList = line.times
//      .map(r => {
//        val userString = r._1 +DELIMITER + r._2 + DELIMITER + r._3
//        userString
//      })
//    (newEvent, newList)
//  }
}
