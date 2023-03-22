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

class CassandraConnectionSetContainment extends Serializable {
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
  private val writeConf: WriteConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)//batchSize = 1, throughputMiBPS = Option(0.5)

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
      println(cassandra_host, cassandra_keyspace_name, cassandra_keyspace_name)
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
        System.out.println("A problem occurred creating the table")
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

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
        System.out.println("A problem occurred dropping the tables")
        e.printStackTrace()
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit = {
    val name = logName + "_set_seq"
//    val prevTable = this.readTableSeq(logName)

    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase,
      tableName = name.toLowerCase(),
      columns = SomeColumns(
        "events" append,
        "sequence_id"
      ),
      writeConf = writeConf)
  }

  def readCombineSequenceIndex(newCombinations:RDD[SetCInverted], logName:String):RDD[SetCInverted] ={
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
      .rdd.map(row=>{
      val event = row.getAs[String]("event_name")
      val ids:List[Long] = row.getAs[Seq[String]]("sequences").map(_.toLong).toList
      SetCInverted(event,ids)
    })
      .keyBy(_.event)
      .fullOuterJoin(newCombinations.keyBy(_.event))
      .map(x=>{
        val s1 = x._2._1.getOrElse(SetCInverted("",List()))
        val s2 = x._2._2.getOrElse(SetCInverted("",List()))
        val l:List[Long] = s1.ids++s2.ids
        val event = if(s2.event==""){
          s1.event
        }else{
          s2.event
        }
        SetCInverted(event,l.distinct)
      })
    df
  }

  def writeTableSequenceIndex(combinations: RDD[SetCInverted], logName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_idx = logName + "_set_idx"
    val combined = this.readCombineSequenceIndex(combinations,logName)
    val table = combined
      .map(r => {
        val formatted = cassandraFormat(r)
        CassandraSetIndex(formatted._1, formatted._2)
      })
    //    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = table_idx.toLowerCase,
      columns = SomeColumns(
        "event_name",
        "sequences"
      ), writeConf
    )
  }

  def cassandraFormat(line: SetCInverted): (String, List[String]) = {
    (line.event, line.ids.map(x => x.toString))
  }


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
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred dropping tables tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  def closeSpark(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("Closing Spark")
    spark.close()

  }

}
