package auth.datalab.siesta.SetContainment

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.SetContainment.SetContainment.SetCInverted
import auth.datalab.siesta.Utils.Utilities
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * This class contains all the communication between preprocessing component of Set-Containment and Cassandra.
 * It is responsible to achieve communication with Cassandra through spark, write and combine the traces with the new
 * ones and also store and merge the single inverted index.
 */
class CassandraConnectionSetContainment extends Serializable {
  /**
   * This case class contains the structure of the single inverted index. It is similar to the one
   * created in [[SetContainment]] but the names are set to match the ones used in Query Processor
   * and also the trace ids is now Strings.
   *
   * @param event_name The event type
   * @param sequences  The trace ids but in Strings
   */
  case class CassandraSetIndex(event_name: String, sequences: List[String])


  private var cassandra_host: String = _
  private var cassandra_port: String = _
  private var cassandra_user: String = _
  private var cassandra_pass: String = _
  private var cassandra_replication_class: String = _
  private var cassandra_replication_rack: String = _
  private var cassandra_replication_factor: String = _
  private var cassandra_keyspace_name: String = _
  private var cassandra_write_consistency_level: String = _
  private var cassandra_gc_grace_seconds: String = _
  private var _configuration: SparkConf = _
  private val DELIMITER = "¦delab¦"
  private val writeConf: WriteConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE) //batchSize = 1, throughputMiBPS = Option(0.5)

  /**
   * Initializes communication between spark and Cassandra using environmental variables
   */
  def startSpark(): Unit = {
    try {
      //loading env variables
      cassandra_host = Utilities.readEnvVariable("cassandra_host")
      cassandra_port = Utilities.readEnvVariable("cassandra_port")
      cassandra_user = Utilities.readEnvVariable("cassandra_user")
      cassandra_pass = Utilities.readEnvVariable("cassandra_pass")
      cassandra_gc_grace_seconds = Utilities.readEnvVariable("cassandra_gc_grace_seconds")
      cassandra_keyspace_name = Utilities.readEnvVariable("cassandra_keyspace_name")
      cassandra_replication_class = Utilities.readEnvVariable("cassandra_replication_class")
      cassandra_replication_rack = Utilities.readEnvVariable("cassandra_replication_rack")
      cassandra_replication_factor = Utilities.readEnvVariable("cassandra_replication_factor")
      cassandra_write_consistency_level = Utilities.readEnvVariable("cassandra_write_consistency_level")
      Logger.getLogger("Initialize Spark").log(Level.INFO, s"$cassandra_host, $cassandra_port, $cassandra_keyspace_name")
    } catch {
      case e: NullPointerException =>
        e.printStackTrace()
        System.exit(1)
    }
    _configuration = new SparkConf()
      .setAppName("FA Indexing")
      //      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandra_host)
      .set("spark.cassandra.auth.username", cassandra_user)
      .set("spark.cassandra.auth.password", cassandra_pass)
      .set("spark.cassandra.connection.port", cassandra_port)
      .set("spark.cassandra.output.consistency.level", cassandra_write_consistency_level)
      .set("spark.cassandra.connection.timeoutMS", "20000")


    val spark = SparkSession.builder().config(_configuration).getOrCreate()
    //create the siesta keyspace if this does not already exists
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("create keyspace if not exists " + cassandra_keyspace_name + " WITH replication = "
          + "{'class':'" + cassandra_replication_class + "', '" + cassandra_replication_rack + "':" + cassandra_replication_factor + "}")
        session.execute("USE " + cassandra_keyspace_name + ";")
      }
    } catch {
      case e: Exception =>
        Logger.getLogger("Creating keyspace").log(Level.ERROR, s"A problem occurred while creating the keyspace")
        print(e.printStackTrace())
        //Stop Spark
        spark.close()
        System.exit(1)
    }

  }

  /**
   * Creates the tables (if they are not already exist) in Cassandra for a given log database. The tables are
   *  - Sequence Table : Contains the traces
   *  - Index Table : Is the main index. The single inverted index.
   *
   * @param logName The name of the log database
   */
  def createTables(logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_idx = logName + "_set_idx"
    val table_seq = logName + "_set_seq"
    val tables: Map[String, String] = Map(
      table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)",
      table_idx -> "event_name text, sequences list<text>, PRIMARY KEY (event_name)"
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
        Logger.getLogger("Create tables").log(Level.ERROR, s"A problem occurred while creating the tables")
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  /**
   * Delete both tables from a specific log database
   *
   * @param logName The name of the log database
   */
  def dropTables(logName: String): Unit = {
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
        Logger.getLogger("Drop tables").log(Level.ERROR, s"A problem occurred while dropping the tables")
        e.printStackTrace()
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  /**
   * Stores the sequence table in Cassandra
   *
   * @param table   The rdd containing the traces
   * @param logName The name of the log database
   */
  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit = {
    val name = logName + "_set_seq"
    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase,
      tableName = name.toLowerCase(),
      columns = SomeColumns(
        "events" append,
        "sequence_id"
      ),
      writeConf = writeConf)
  }

  /**
   * Loads the previously stored inverted index from Cassandra and combines it with
   * the newly computed inverted index, making sure all the trace ids that correspond
   * to a specific event type are sorted in ascending order
   *
   * @param newCombinations
   * @param logName
   * @return
   */
  def readCombineSequenceIndex(newCombinations: RDD[SetCInverted], logName: String): RDD[SetCInverted] = {
    val spark = SparkSession.builder().getOrCreate()
    val table_idx = logName + "_set_idx"
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> table_idx,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()
      .rdd.map(row => {
      val event = row.getAs[String]("event_name")
      val ids: List[Long] = row.getAs[Seq[String]]("sequences").map(_.toLong).toList
      SetCInverted(event, ids)
    })
      .keyBy(_.event)
      //only keep those that changed so only the necessary information are transmitted
      .fullOuterJoin(newCombinations.keyBy(_.event))
      .map(x => {
        val s1 = x._2._1.getOrElse(SetCInverted("", List()))
        val s2 = x._2._2.getOrElse(SetCInverted("", List()))
        val l: List[Long] = s1.ids ++ s2.ids
        val event = if (s2.event == "") {
          s1.event
        } else {
          s2.event
        }
        SetCInverted(event, l.distinct.sortWith((l1, l2) => l1 < l2))
      })
    df
  }

  /**
   * Stores the inverted index in Cassandra
   *
   * @param table   The RDD containing the newly computed
   * @param logName The name of the log database
   */
  def writeTableSequenceIndex(combinations: RDD[SetCInverted], logName: String): Unit = {
    val table_idx = logName + "_set_idx"
    //combine the single inverted index with the one that is previously stored (if any)
    val combined = this.readCombineSequenceIndex(combinations, logName)

    //transform the structure and store them
    val table = combined
      .map(r => {
        val formatted = cassandraFormat(r)
        CassandraSetIndex(formatted._1, formatted._2)
      })
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = table_idx.toLowerCase,
      columns = SomeColumns(
        "event_name",
        "sequences"
      ), writeConf
    )
  }

  /**
   * Transform [[SetCInverted]] -> [[CassandraSetIndex]]
   *
   * @param line
   * @return
   */
  def cassandraFormat(line: SetCInverted): (String, List[String]) = {
    (line.event, line.ids.map(x => x.toString))
  }


  /**
   * Delete all tables in the defined keyspace
   */
  def dropAlltables(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.getMetadata
          .getKeyspace(this.cassandra_keyspace_name)
          .get().getTables.asScala
          .map(x => x._2.getName.toString)
          .foreach(table => {
            session.execute("drop table if exists " + this.cassandra_keyspace_name + '.' + table + ";")
          })
        session.close()
      }
    } catch {
      case e: Exception =>
        Logger.getLogger("Drop tables").log(Level.ERROR, s"A problem occurred dropping tables tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  /**
   * Close spark connection
   */
  def closeSpark(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("Closing Spark")
    spark.close()

  }

}
