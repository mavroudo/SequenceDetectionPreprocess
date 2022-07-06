package auth.datalab.sequenceDetection.Signatures

import auth.datalab.sequenceDetection.{CassandraConnectionTrait, Structs}
import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
class CassandraConnection extends Serializable with CassandraConnectionTrait {
  case class withList(id:String,signature: Map[Int,String],sequence_ids:List[String])

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
        session.execute(s"create index ${table_signatures}_index on ${this.cassandra_keyspace_name+"."+table_signatures} (entries( signature ));")
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
    val table_seq = logName + "_sign_seq"
    val table_meta = logName + "_sign_meta"
    val table_signatures = logName + "_sign_idx"
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_signatures + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_seq + ";")
        session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table_meta + ";")
        session.execute("DROP INDEX IF EXISTS " + cassandra_keyspace_name + "." + table_signatures+"_index" + ";")

      }
    }
    catch {
      case e: Exception =>
        System.out.println("A problem occurred dropping the tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit = {
    val table_seq = logName + "_sign_seq"
    table.saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = table_seq.toLowerCase(), writeConf = writeConf)
  }

  def writeTableSign(table: RDD[Signatures.Signatures], logName: String): Unit = {
    val table_signatures = logName + "_sign_idx"
    table
      .map(x=>{
        var n = Map[Int, String]()
        for(i <- 0 until x.signature.length){
          n+=(i->x.signature(i).toString)
        }
        withList(x.signature,n,x.sequence_ids)
      })
    .saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase,
      tableName = table_signatures.toLowerCase(),
      columns = SomeColumns(
        "id",
        "signature",
        "sequence_ids" append
      ),
      writeConf = writeConf)
  }

  def writeTableMetadata(events: List[String], topKfreqPairs: List[(String, String)], logName: String): Unit = {
    val table_meta = logName + "_sign_meta"
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        val prepared:PreparedStatement = session.prepare(s"insert into ${this.cassandra_keyspace_name+"."+table_meta} (object,list) values (?,?)")
        session.execute(prepared.bind("events",events.asJava))
        session.execute(prepared.bind("pairs",topKfreqPairs.map(x=>s"${x._1},${x._2}").asJava))
      }
    }
    catch {
      case e: Exception =>
        System.out.println("A problem occurred while saving metadata")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }


}
