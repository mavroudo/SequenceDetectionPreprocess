package auth.datalab.sequenceDetection

import com.datastax.driver.core.{Cluster, ConsistencyLevel}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.InetSocketAddress

trait CassandraConnectionTrait {
  protected var cassandra_host: String = null
  protected var cassandra_port: String = null
  protected var cassandra_user: String = null
  protected var cassandra_pass: String = null
  protected var cassandra_replication_class: String = null
  protected var cassandra_replication_rack: String = null
  protected var cassandra_replication_factor: String = null
  protected var cassandra_keyspace_name: String = null
  protected var cassandra_write_consistency_level: String = null
  protected var cassandra_gc_grace_seconds: String = null
  protected var _configuration: SparkConf = null
  protected val DELIMITER = "¦delab¦"
  protected val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE, batchSize = 1, throughputMiBPS = 0.5)

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


      println(cassandra_host,cassandra_keyspace_name,cassandra_keyspace_name)
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

    try {
      val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(this.cassandra_host, this.cassandra_port.toInt)).withCredentials(this.cassandra_user, this.cassandra_pass).build()
      val session = cluster.connect(this.cassandra_keyspace_name)
      var tables_iterator = cluster.getMetadata.getKeyspace(this.cassandra_keyspace_name).getTables.iterator()
      while (tables_iterator.hasNext) {
        session.execute("drop table if exists " + this.cassandra_keyspace_name + '.' + tables_iterator.next.getName() + ";")
      }
      session.close()
      cluster.close()

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

  def closeSpark(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("Closing Spark")
    spark.close()

  }

}
