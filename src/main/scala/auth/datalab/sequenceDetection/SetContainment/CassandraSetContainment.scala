package auth.datalab.sequenceDetection.SetContainment


import auth.datalab.sequenceDetection.SetContainment.SetContainment.SetCInverted
import auth.datalab.sequenceDetection.{CassandraConnectionTrait, Structs, Utils}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class CassandraSetContainment extends Serializable with CassandraConnectionTrait {

  def createTable(logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_idx = logName + "_set_idx"
    val table_seq = logName + "_set_seq"
    val tables: Map[String, String] = Map(
      table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)",
      table_idx -> "event1_name text, event2_name text, sequences list<text>, PRIMARY KEY (event1_name, event2_name)"
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
    val table_idx = logName + "_set_idx"
    val table_seq = logName + "_set_seq"
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_idx + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_seq + ";")

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

  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit = {
    val name = logName + "_set_seq"
    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = name.toLowerCase(), writeConf = writeConf)
  }

  def writeTableSequenceIndex(combinations: RDD[SetCInverted], logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_idx = logName + "_set_idx"
    val table = combinations
      .map(r => {
        val formatted = cassandraFormat(r)
        Structs.CassandraIndex(formatted._1, formatted._2, formatted._3)
      })
    //    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = table_idx.toLowerCase,
      columns = SomeColumns(
        "event1_name",
        "event2_name",
        "sequences" append
      ), writeConf
    )
  }

  def cassandraFormat(line: SetCInverted): (String, String, List[String]) = {
    (line.event1, line.event2, line.ids.map(x => x.toString))
  }


}
