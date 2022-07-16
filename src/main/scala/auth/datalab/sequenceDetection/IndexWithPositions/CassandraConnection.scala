package auth.datalab.sequenceDetection.IndexWithPositions

import auth.datalab.sequenceDetection.IndexWithPositions.IndexingNoTimestamps.{CassStoreWithPositions, EventIdTimeListsEnhanced, SequenceWithPositions}
import auth.datalab.sequenceDetection.Structs
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class CassandraConnection(table_name: String) extends auth.datalab.sequenceDetection.SIESTA.CassandraConnection {

  val table_temp: String = table_name + "_temp"
  val table_seq: String = table_name + "_seq"
  val table_idx: String = table_name + "_idx"
  val table_count: String = table_name + "_count"
  val table_single: String = table_name + "_single"

  val tables: Map[String, String] = Map(
    table_idx -> "event1_name text, event2_name text, sequences list<text>, PRIMARY KEY (event1_name, event2_name)",
    table_temp -> "event1_name text, event2_name text,  sequences list<text>, PRIMARY KEY (event1_name, event2_name)",
    table_count -> "event1_name text, sequences_per_field list<text>, PRIMARY KEY (event1_name)",
    table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)",
    table_single -> "event_name text, sequences list<text>, PRIMARY KEY (event_name)"
  )


  def writeTableIndex(combinations: RDD[EventIdTimeListsEnhanced],name:String): Unit ={
    val c = combinations.map(x=>{
      CassStoreWithPositions(x.event1,x.event2,x.positions)
    })

    val table = c
      .map(r => {
        val formatted = combinationsToCassandraFormat(r)
        Structs.CassandraIndex(formatted._1, formatted._2, formatted._3)
      })
    table.persist(StorageLevel.MEMORY_AND_DISK)
    table.saveToCassandra(
      keyspaceName = this.cassandra_keyspace_name.toLowerCase(),
      tableName = name.toLowerCase,
      columns = SomeColumns(
        "event1_name",
        "event2_name",
        "sequences" append //Method to append to a list in cassandra
      ), writeConf
    )
    table.unpersist()
  }

  def dropTables():Unit={
    this.dropTables(List(table_idx, table_seq, table_temp, table_count, table_single))
  }
  def createTables():Unit={
    this.createTables(tables)
  }

  def combinationsToCassandraFormat(line: CassStoreWithPositions): (String, String, List[String]) = {
    val newList = line.times
      .map(r => {
        val userString = new StringBuffer()
        userString.append(r.id + "(")
        for (i <- 1 until r.positions.length by 2) {
          userString.append(s"""(${r.positions(i-1)},${r.positions(i)}),""")
        }
        userString.toString.dropRight(1)+")"
      })
    (line.event1, line.event2, newList)
  }


  }
