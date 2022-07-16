package auth.datalab.sequenceDetection.SIESTA

import auth.datalab.sequenceDetection.CommandLineParser.Utilities.Iterations
import auth.datalab.sequenceDetection.CommandLineParser.{Config, Utilities}
import auth.datalab.sequenceDetection.{Structs, TraceGenerator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object SIESTA {
  private var cassandraConnection: CassandraConnection = _

  def execute(c: Config): Unit = {
    val table_name = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    val table_temp = table_name + "_temp"
    val table_seq = table_name + "_seq"
    val table_idx = table_name + "_idx"
    val table_count = table_name + "_count"
    val table_one = table_name + "_one"

    val tables: Map[String, String] = Map(
      table_idx -> "event1_name text, event2_name text, sequences list<text>, PRIMARY KEY (event1_name, event2_name)",
      table_temp -> "event1_name text, event2_name text,  sequences list<text>, PRIMARY KEY (event1_name, event2_name)",
      table_count -> "event1_name text, sequences_per_field list<text>, PRIMARY KEY (event1_name)",
      table_seq -> "sequence_id text, events list<text>, PRIMARY KEY (sequence_id)",
      table_one -> "event_name text, sequences list<text>, PRIMARY KEY (event_name)"
    )
    //    Connect with cassandra and initialize the tables
    cassandraConnection = new CassandraConnection()
    cassandraConnection.startSpark()
    if (c.delete_previous) {
      cassandraConnection.dropTables(List(table_idx, table_seq, table_temp, table_count, table_one))
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables(tables)

    try {
      val init = Utilities.getRDD(c,100)
      val sequencesRDD_before_repartitioned=init.data
      val traces: Int = Utilities.getTraces(c,sequencesRDD_before_repartitioned)
      val iterations:Iterations = Utilities.getIterations(c,sequencesRDD_before_repartitioned,traces)
      val ids: List[Array[Long]] = Utilities.getIds(c,sequencesRDD_before_repartitioned,traces,iterations.iterations)
      var k = 0L
      for (id <- ids) {
        val sequencesRDD: RDD[Structs.Sequence]=Utilities.getNextData(c,sequencesRDD_before_repartitioned,id,init.traceGenerator,iterations.allExecutors)
        k = k + Preprocess.preprocess(sequencesRDD, cassandraConnection, table_name, c)
      }
      println(s"Time taken: $k ms")
      cassandraConnection.closeSpark()
      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println("ALL RESULTS IN MB")
      println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      println("** Free Memory:  " + runtime.freeMemory / mb)
      println("** Total Memory: " + runtime.totalMemory / mb)
      println("** Max Memory:   " + runtime.maxMemory / mb)
    } catch {
      case e: Exception =>
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()
    }

  }

}
