package auth.datalab.sequenceDetection.SIESTA

import auth.datalab.sequenceDetection.{Structs, TraceGenerator, Utils}
import auth.datalab.sequenceDetection.CommandLineParser.Config
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
      //      read/generates data
      val spark = SparkSession.builder().getOrCreate()
      var traceGenerator: TraceGenerator = null
      val sequencesRDD_before_repartitioned: RDD[Structs.Sequence] = {
        if (c.filename != "synthetic") {
          Utils.readLog(c.filename)
        } else {
          traceGenerator = new TraceGenerator(c.traces, c.event_types, c.length_min, c.length_max)
          traceGenerator.produce((1 to 50).toList)
        }
      }
      val traces: Int = {
        if (c.filename == "synthetic") c.traces else sequencesRDD_before_repartitioned.count().toInt
      }
      val allExecutors = spark.sparkContext.getExecutorMemoryStatus.keys.size
      val iterations: Int = { // determines iterations
        if (c.iterations != -1) c.iterations
        else {
          val minExecutorMemory = spark.sparkContext.getExecutorMemoryStatus.map(_._2._1).min
          val average_length = sequencesRDD_before_repartitioned.takeSample(withReplacement = false, 50).map(_.events.size).sum / 50
          val size_estimate_trace: scala.math.BigInt = SizeEstimator.estimate(sequencesRDD_before_repartitioned.take(1)(0).events.head) * average_length * (average_length / 2)
          val partitionNumber = if (minExecutorMemory / size_estimate_trace > traces) 1 else ((size_estimate_trace * traces) / minExecutorMemory).toInt + 1
          partitionNumber / allExecutors
        }
      }
      val ids: List[Array[Long]] = { // ids per iteration
        if (c.filename == "synthetic") {
          sequencesRDD_before_repartitioned
            .map(_.sequence_id)
            .collect().sortWith((x, y) => x < y)
            .sliding(traces / iterations, traces / iterations)
            .toList
        } else {
          (0L to traces).toArray
            .sliding(traces / iterations, traces / iterations).toList
        }
      }
      var k = 0L
      for (id <- ids) {
        val sequencesRDD: RDD[Structs.Sequence] = {
          if (c.filename == "synthetic") {
            traceGenerator.produce(id).repartition(allExecutors)
          } else {
            sequencesRDD_before_repartitioned
              .filter(x => x.sequence_id >= id(0) && x.sequence_id <= id.last)
              .repartition(allExecutors)
          }
        }
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
