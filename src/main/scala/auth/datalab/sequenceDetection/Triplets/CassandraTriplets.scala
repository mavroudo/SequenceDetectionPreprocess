package auth.datalab.sequenceDetection.Triplets

import auth.datalab.sequenceDetection.{CassandraConnectionTrait, Structs}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class CassandraTriplets extends Serializable with CassandraConnectionTrait {

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

  def writeTableSeq(table: RDD[Structs.Sequence], logName: String): Unit = {
    val table_seq = logName + "_trip_seq"
    table.filter(_.events.nonEmpty).saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = table_seq.toLowerCase(), columns = SomeColumns("events", "sequence_id"), writeConf = writeConf)
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
          userString = userString + "(" + r.times(i - 2) + "," + r.times(i - 1) + "," + r.times(i) + "),"
        }
        userString = userString.dropRight(1) + ")"
        userString
      })
    (line.event1, line.event2, line.event3, newList)
  }


  def writeTableSeqCount(combinations: RDD[Structs.TripleCountList], logname: String): Unit = {
    val tableName = logname + "_trip_count"
    val spark = SparkSession.builder().getOrCreate()
    val table_load = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> tableName,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()

    val tlTransofmed = table_load.rdd.flatMap(x => {
      val event1 = x.getAs[String]("event1_name")
      val event2 = x.getAs[String]("event2_name")
      val array = x.getAs[Seq[String]]("sequences_per_field")
      array.map(y => {
        val values: List[String] = y.split("¦delab¦").toList
        ((event1, event2, values.head), values(1).toLong, values(2).toInt)
      })
    })
      .keyBy(_._1)

    val tTransofmed = combinations.flatMap(x => {
      val event1 = x.event1_name
      val event2 = x.event2_name
      x.times.map(y => {
        ((event1, event2, y._1), y._2, y._3)
      })
    })
      .keyBy(_._1)
      .fullOuterJoin(tlTransofmed)
      .map(x => {
        val t1 = x._2._1.getOrElse(("", 0L, 0))._3
        val t2 = x._2._2.getOrElse(("", 0L, 0))._3
        val d1 = x._2._1.getOrElse(("", 0L, 0))._2
        val d2 = x._2._2.getOrElse(("", 0L, 0))._2
        val average_duration = (d1 * t1 + d2 * t2) / (t1 + t2)
        ((x._1._1, x._1._2), x._1._3, average_duration, t1 + t2)
      })
      .keyBy(_._1)
      .groupByKey()
      .map(x => {
        val times = x._2.toList.map(y => {
          (y._2, y._3, y._4)
        })
        Structs.TripleCountList(x._1._1, x._1._2, times)
      })

    val table = tTransofmed
      .map(r => {
        val formatted = combinationsCountToCassandraFormat(r)
        Structs.CassandraCountTriplet(formatted._1, formatted._2, formatted._3)
      })
    table.persist(StorageLevel.MEMORY_AND_DISK)
    table
      .saveToCassandra(
        cassandra_keyspace_name.toLowerCase,
        tableName.toLowerCase,
        SomeColumns("event1_name","event2_name", "sequences_per_field" )
      )
    table.unpersist()
  }


    private def combinationsCountToCassandraFormat(line: Structs.TripleCountList): (String, String, List[String]) = {
      val e1 = line.event1_name
      val e2= line.event2_name
      val newList = line.times
        .map(r => {
          val userString = r._1 +DELIMITER + r._2 + DELIMITER + r._3
          userString
        })
      (e1,e2, newList)
    }
}
