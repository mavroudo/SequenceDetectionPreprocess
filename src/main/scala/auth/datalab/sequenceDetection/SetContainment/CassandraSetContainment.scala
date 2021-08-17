package auth.datalab.sequenceDetection.SetContainment


import auth.datalab.sequenceDetection.SetContainment.SetContainment.SetCInverted
import auth.datalab.sequenceDetection.{Structs, Utils}
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.querybuilder.QueryBuilder.append
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class CassandraSetContainment extends Serializable {

  private var cassandra_host: String = null
  private var cassandra_port: String = null
  private var cassandra_user: String = null
  private var cassandra_pass: String = null
  private var cassandra_replication_class: String = null
  private var cassandra_replication_rack: String = null
  private var cassandra_replication_factor: String = null
  private var cassandra_keyspace_name: String = null
  private var cassandra_write_consistency_level: String = null
  private var cassandra_gc_grace_seconds: String = null
  private var _configuration: SparkConf = null
  private val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)

  def startSpark(): Unit = {
    try {
      cassandra_host = Utils.readEnvVariable("cassandra_host")
      cassandra_port = Utils.readEnvVariable("cassandra_port")
      cassandra_user = Utils.readEnvVariable("cassandra_user")
      cassandra_pass = Utils.readEnvVariable("cassandra_pass")
      cassandra_gc_grace_seconds = Utils.readEnvVariable("cassandra_gc_grace_seconds")
      cassandra_keyspace_name = Utils.readEnvVariable("cassandra_keyspace_name") // we use different variable
      cassandra_replication_class = Utils.readEnvVariable("cassandra_replication_class")
      cassandra_replication_rack = Utils.readEnvVariable("cassandra_replication_rack")
      cassandra_replication_factor = Utils.readEnvVariable("cassandra_replication_factor")
      cassandra_write_consistency_level = Utils.readEnvVariable("cassandra_write_consistency_level")


      println(cassandra_host, cassandra_keyspace_name, cassandra_keyspace_name)
    } catch {
      case e: NullPointerException =>
        e.printStackTrace()
        System.exit(1)
    }
    _configuration = new SparkConf()
      .setAppName("FA Indexing")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandra_host)
      .set("spark.cassandra.auth.username", cassandra_user)
      .set("spark.cassandra.auth.password", cassandra_pass)
      .set("spark.cassandra.connection.port", cassandra_port)
      .set("spark.cassandra.output.consistency.level", cassandra_write_consistency_level)


    val spark = SparkSession.builder().config(_configuration).getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("create keyspace if not exists " + cassandra_keyspace_name + " WITH replication = "
          + "{'class':'" + cassandra_replication_class + "', '" + cassandra_replication_rack + "':" + cassandra_replication_factor + "}")
        session.execute("USE " + cassandra_keyspace_name + ";")
      }
    } catch {
      case e: Exception =>
        System.out.println("A problem occurred creating the keyspace")
        print(e.printStackTrace())
        //Stop Spark
        spark.close()
        System.exit(1)
    }

  }

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
    val table_idx = logName + "_idx"
    val table_seq = logName + "_seq"
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
//    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE) //this needs to be tested
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

  def closeSpark(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("Closing Spark")
    spark.close()

  }


}
