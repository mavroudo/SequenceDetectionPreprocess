package auth.datalab.sequenceDetection

import java.net.InetSocketAddress


import com.datastax.driver.core.{Cluster, ConsistencyLevel, KeyspaceMetadata, Session, TableMetadata}

import org.apache.spark.SparkConf
import org.apache.spark.sql._

import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf


class CassandraConnection extends Serializable {


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


  def startSpark(): Unit = {
    try {
      cassandra_host = Utils.readEnvVariable("cassandra_host")
      cassandra_port = Utils.readEnvVariable("cassandra_port")
      cassandra_user = Utils.readEnvVariable("cassandra_user")
      cassandra_pass = Utils.readEnvVariable("cassandra_pass")
      cassandra_gc_grace_seconds = Utils.readEnvVariable("cassandra_gc_grace_seconds")
      cassandra_keyspace_name = Utils.readEnvVariable("cassandra_keyspace_name")
      cassandra_replication_class = Utils.readEnvVariable("cassandra_replication_class")
      cassandra_replication_rack = Utils.readEnvVariable("cassandra_replication_rack")
      cassandra_replication_factor = Utils.readEnvVariable("cassandra_replication_factor")
      cassandra_write_consistency_level = Utils.readEnvVariable("cassandra_write_consistency_level")
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

  def dropAlltables() = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(this.cassandra_host, this.cassandra_port.toInt)).withCredentials(this.cassandra_user, this.cassandra_pass).build()
    val session = cluster.connect(this.cassandra_keyspace_name)
    try {
      var tables_iterator = cluster.getMetadata.getKeyspace(this.cassandra_keyspace_name).getTables.iterator()
      while (tables_iterator.hasNext) {
        session.execute("drop table if exists " + this.cassandra_keyspace_name + '.' + tables_iterator.next.getName() + ";")
      }
    } catch {
      case e: Exception =>
        System.out.println("A problem occurred while reading tables")
        e.printStackTrace()
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }


  /**
   * Method for dropping tables in cassandra
   *
   * @param names A list of all table names to be dropped
   */
  def dropTables(names: List[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- names) {
          session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table + ";")
        }
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

  /**
   * Method for creating tables in cassandra
   *
   * @param names A map with the names of the tables and the details for each one
   */
  def createTables(names: Map[String, String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- names) {
          session.execute("CREATE TABLE IF NOT EXISTS " + cassandra_keyspace_name + "." +
            table._1 + " (" + table._2 + ") " +
            "WITH GC_GRACE_SECONDS=" + cassandra_gc_grace_seconds +
            ";")
        }
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        System.out.println("A problem occurred creating the tables")
        //Stop Spark
        spark.close()
        System.exit(1)
    }
  }

  /**
   * Write an RDD of user sequences to cassandra or csv
   *
   * @param table The data RDD
   * @param name  The table name
   */
  def writeTableSeq(table: RDD[Structs.Sequence], name: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE) //this needs to be tested
    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = name.toLowerCase(), writeConf = writeConf)
  }

  def writeTableSequenceIndex(combinations: RDD[Structs.EventIdTimeLists], name: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val table = combinations
      .map(r => {
        val formatted = combinationsToCassandraFormat(r)
        Structs.CassandraIndex(formatted._1, formatted._2, formatted._3)
      })
    val time = System.currentTimeMillis() //TODO: add one time at the end to know the time
    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = name.toLowerCase,
      columns = SomeColumns(
        "event1_name",
        "event2_name",
        "sequences" append //Method to append to a list in cassandra
      ), writeConf
    )
  }

  private def combinationsToCassandraFormat(line: Structs.EventIdTimeLists): (String, String, List[String]) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val newList = line.times
      .map(r => {
        var userString = r.id + "("

        //DEBUG
        //          count+= r.times.length
        //END DEBUG

        for (i <- 1 until r.times.length by 2) {
          userString = userString + "(" + r.times(i - 1) + "," + r.times(i) + "),"
        }
        userString = userString.dropRight(1) + ")"
        userString
      })
    (line.event1, line.event2, newList)

  }

  def closeSpark(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("Closing Spark")
    spark.close()

  }
}

