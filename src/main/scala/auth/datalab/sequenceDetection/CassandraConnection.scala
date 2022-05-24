package auth.datalab.sequenceDetection

import java.net.InetSocketAddress
import java.sql.Timestamp

import com.datastax.driver.core.{Cluster, ConsistencyLevel, KeyspaceMetadata, Session, TableMetadata}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class CassandraConnection extends Serializable with CassandraConnectionTrait {

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
    //    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE) //this needs to be tested
    table.filter(_.events.nonEmpty).saveToCassandra(keyspaceName = this.cassandra_keyspace_name.toLowerCase, tableName = name.toLowerCase(), columns = SomeColumns("events", "sequence_id"), writeConf = writeConf)
  }

  def writeTableSequenceIndex(combinations: RDD[Structs.EventIdTimeLists], name: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table = combinations
      .map(r => {
        val formatted = combinationsToCassandraFormat(r)
        Structs.CassandraIndex(formatted._1, formatted._2, formatted._3)
      })
    //    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE)
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

  def writeTableOne(data: RDD[Structs.InvertedOne], name: String): Unit = {
    val table = data
      .map(r => {
        val formatted = invertedOneToCassandraFormat(r)
        Structs.CassandraIndexOne(formatted._1, formatted._2)
      })
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = name.toLowerCase,
      columns = SomeColumns(
        "event_name",
        "sequences" append //Method to append to a list in cassandra
      ), writeConf)
  }

  def writeTableSeqCount(combinations: RDD[Structs.CountList], tableName: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val table_load = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> tableName,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()

    val tlTransofmed = table_load.rdd.flatMap(x=>{
      val prim_event = x.getAs[String]("event1_name")
      val array=x.getAs[Seq[String]]("sequences_per_field")
      array.map(y=>{
        val values:List[String] = y.split("¦delab¦").toList
        ((prim_event,values.head),values(1).toLong,values(2).toInt)
      })
    })
      .keyBy(_._1)

    val tTransofmed = combinations.flatMap(x=>{
      val prim_event=x.event1_name
      x.times.map(y=>{
        ((prim_event,y._1),y._2,y._3)
      })
    })
      .keyBy(_._1)
      .fullOuterJoin(tlTransofmed)
      .map(x=>{
        val t1=x._2._1.getOrElse(("",0L,0))._3
        val t2=x._2._2.getOrElse(("",0L,0))._3
        val d1=x._2._1.getOrElse(("",0L,0))._2
        val d2=x._2._2.getOrElse(("",0L,0))._2
        val average_duration=(d1*t1+d2*t2)/(t1+t2)
        (x._1._1,x._1._2,average_duration,t1+t2)
      })
      .keyBy(_._1)
      .groupByKey()
      .map(x=>{
        val times=x._2.map(y=>{
          (y._2,y._3,y._4)
        })
        Structs.CountList(x._1,times.toList)
      })





    val table = tTransofmed
      .map(r => {
        val formatted = combinationsCountToCassandraFormat(r)
        Structs.CassandraCount(formatted._1, formatted._2)
      })
    table.persist(StorageLevel.MEMORY_AND_DISK)
    table
      .saveToCassandra(
        cassandra_keyspace_name.toLowerCase,
        tableName.toLowerCase,
        SomeColumns("event1_name", "sequences_per_field" append)
      )
    table.unpersist()
  }

  /**
   * Method to read the data from a cassandra table
   *
   * @param name Table name
   * @return A Dataframe with the table data
   */
  def readTable(name: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val table = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> name,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()
    table
  }

  def readTemp(temp: String, funnel_date: Timestamp): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val tempTable = this.readTable(temp)
      .rdd
      .flatMap(row => {
        val res = ListBuffer[(String, String, String, String)]()
        row
          .getAs[mutable.WrappedArray[String]](2)
          .toList
          .foreach(l => {
            val split = l.split(DELIMITER)
            res.+=((row.getString(0), row.getString(1), split(0), split(1).replace("(", "").replace(")", "")))
          })
        res
      })
      .filter(row => {
        Utils.compareTimes(funnel_date.toString, row._4)
      })
      .toDF("ev1", "ev2", "id", "time")
    //      .persist(StorageLevel.DISK_ONLY)
    //cache it
    tempTable.count()
    tempTable
  }

  def writeTableSeqTemp(table: RDD[Structs.EventIdTimeLists], name: String): Unit = {
    val latest_times = table.map(row => {
      val users = row.times
        .map(p => {
          val user = p.id
          val time = p.times.last.trim
          Structs.IdTimeList(user, List(time))
        })
      Structs.EventIdTimeLists(row.event1, row.event2, users)
    })

    val toWrite = latest_times
      .map(r => {
        val formatted = detailsToCassandraFormat(r)
        Structs.CassandraIndex(formatted._1, formatted._2, formatted._3)
      })

    toWrite.saveToCassandra(
      keyspaceName = cassandra_keyspace_name.toLowerCase,
      tableName = name.toLowerCase,
      columns = SomeColumns(
        "event1_name",
        "event2_name",
        "sequences"
      )
    )
  }


  /**
   * Method for transforming the details index
   * to a common format with the querying code
   *
   * @param line A row of data
   * @return The formatted row
   */
  private def detailsToCassandraFormat(line: Structs.EventIdTimeLists): (String, String, List[String]) = {
    val newList = line.times
      .map(r => {
        var userString = r.id + DELIMITER + "("
        r.times.foreach(l => userString = userString + l + ",")
        userString = userString.dropRight(1) + ")"
        userString
      })
    (line.event1, line.event2, newList)
  }

  private def invertedOneToCassandraFormat(line: Structs.InvertedOne): (String, List[String]) = {
    val newList = line.times
      .map(r => {
        var userString = r.id + "("
        for (i <- r.times.indices) {
          userString = userString + r.times(i) +  ","
        }
        userString = userString.dropRight(1) + ")"
        userString
      })
    (line.event_name, newList)
  }

  private def combinationsToCassandraFormat(line: Structs.EventIdTimeLists): (String, String, List[String]) = {
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

  /**
   * Method for transforming the precomputed combinations counts
   * to a common format with the querying code
   *
   * @param line A row of data
   * @return The formatted row
   */
  private def combinationsCountToCassandraFormat(line: Structs.CountList): (String, List[String]) = {
    val newEvent = line.event1_name
    val newList = line.times
      .map(r => {
        val userString = r._1 + DELIMITER + r._2 + DELIMITER + r._3
        userString
      })
    (newEvent, newList)
  }

}

