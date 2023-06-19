package auth.datalab.siesta.Singatures

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.Utils.Utilities
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.language.postfixOps //added that test if it works
import scala.collection.JavaConverters._


/**
 * This class contains all the communication between preprocessing component of Signature and Cassandra.
 * It is responsible to achieve communication with Cassandra through spark, write and combine the traces with the new
 * ones, store and retrieve metadata and finally store and merge the signatures.
 */
class CassandraConnectionSignatures extends Serializable {
  /**
   * This class holds an intermediate structure that contains for a single signature, a list of all the trace ids
   * that have this signature and also the signature as a bit array that will be latter indexed efficiently by Cassandra.
   * @param id The signature in string format
   * @param signature The signature in a map [position -> 0 or 1]
   * @param sequence_ids The list with all the trace ids that have this singature, in sting format
   */
  private case class withList(id: String, signature: Map[Int, String], sequence_ids: List[String])

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
  private val writeConf: WriteConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE) // batchSize = 1, throughputMiBPS = Option(0.5)

  /**
   * Initializes communication between spark and Cassandra using environmental variables
   */
  def startSpark(): Unit = {
    try {
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
        spark.close()
        System.exit(1)
    }

  }


  /**
   * Creates the tables (if they are not already exist) in Cassandra for a given log database. The tables are
   *  - Metadata Table: Maintains the metadata for the log database
   *  - Sequence Table : Contains the traces
   *  - Signature Table : Is the main index with the signatures
   *
   * @param logName The name of the log database
   */

  def createTables(logName: String): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    val table_seq = logName + "_sign_seq"
    val table_meta = logName + "_sign_meta"
    val table_signatures = logName + "_sign_idx"
    val tables: Map[String, String] = Map(
      table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)",
      table_meta -> "object text, list list<text>, PRIMARY KEY (object)",
      table_signatures -> "id text, signature map<int,text>, sequence_ids list<text>, PRIMARY KEY (id)"
    )
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- tables) {
          session.execute("CREATE TABLE IF NOT EXISTS " + cassandra_keyspace_name + "." +
            table._1 + " (" + table._2 + ") " +
            "WITH GC_GRACE_SECONDS=" + cassandra_gc_grace_seconds +
            ";")
        }
        session.execute(s"create index if not exists ${table_signatures}_index on ${this.cassandra_keyspace_name + "." + table_signatures} (entries( signature ));")
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
   * Delete all 3 tables from a specific log database
   *
   * @param logName The name of the log database
   */
  def dropTables(logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_seq = logName + "_sign_seq"
    val table_meta = logName + "_sign_meta"
    val table_signatures = logName + "_sign_idx"
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_signatures + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_seq + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_meta + ";")
        session.execute("DROP INDEX IF EXISTS " + cassandra_keyspace_name + "." + table_signatures + "_index" + ";")

      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Drop tables").log(Level.ERROR, s"A problem occurred while dropping the tables")
        e.printStackTrace()
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
    val table_seq = logName + "_sign_seq"
    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase,
      tableName = table_seq.toLowerCase(),
      columns = SomeColumns(
        "events" append,
        "sequence_id"
      ),
      writeConf = writeConf)
  }

  /**
   * Retrieves the already stored traces from the Sequence Table in Cassandra.
   *
   * @param logName The log database name
   * @return The already indexed traces
   */
  def readTableSeq(logName: String): RDD[Structs.Sequence] = {
    val table_seq = logName + "_sign_seq"
    val spark = SparkSession.builder().getOrCreate()
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> table_seq,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()
      .rdd.map(row => { //creates the RDD of Sequences
      val sequence_id = row.getAs[String]("sequence_id").toLong
      val events = row.getAs[Seq[String]]("events").map(e => {
        val s = e.replace("Event(", "").replace(")", "").split(",")
        Structs.Event(s(0), s(1))
      }).toList
      Structs.Sequence(events, sequence_id)
    })
  }

  /**
   * Stores the index based on signature in Cassandra
   *
   * @param table   The RDD containing the traces grouped based on their signatures
   * @param logName The name of the log database
   */
  def writeTableSign(table: RDD[Signatures.Signatures], logName: String): Unit = {
    val table_signatures = logName + "_sign_idx"
    table
      .map(x => {
        var n = Map[Int, String]()
        //extracts the bitmap from the string in order to use Cassandra's indexing mechanisms for faster queries
        for (i <- 0 until x.signature.length) {
          n += (i -> x.signature(i).toString)
        }
        withList(x.signature, n, x.sequence_ids)
      })
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase,
        tableName = table_signatures.toLowerCase(),
        columns = SomeColumns(
          "id",
          "signature",
          "sequence_ids"
        ),
        writeConf = writeConf)
  }

  /**
   * Stores metadata for a specific log database in Cassandra
   *
   * @param events        The available event types in the traces
   * @param topKfreqPairs The most frequent event type pairs
   * @param logName       The name of the log database
   */
  def writeTableMetadata(events: List[String], topKfreqPairs: List[(String, String)], logName: String): Unit = {
    val table_meta = logName + "_sign_meta"
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        val prepared: PreparedStatement = session.prepare(s"insert into ${this.cassandra_keyspace_name + "." + table_meta} (object,list) values (?,?)")
        session.execute(prepared.bind("events", events.asJava))
        session.execute(prepared.bind("pairs", topKfreqPairs.map(x => s"${x._1},${x._2}").asJava))
      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Metadata").log(Level.ERROR, s"A problem occurred while saving metadata")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  /**
   * Retrieves metadata from Cassandra
   *
   * @param logName The log database name
   * @return The metadata
   */
  def loadTableMetadata(logName: String): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val table_meta = logName + "_sign_meta"
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> table_meta,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()
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
    Logger.getLogger("Spark").log(Level.INFO, s"Closing spark")
    spark.close()

  }


}
