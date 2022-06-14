package auth.datalab.sequenceDetection.SetContainment

import auth.datalab.sequenceDetection.SequenceDetectionBigData.cassandraConnection
import auth.datalab.sequenceDetection.SetContainment.SetContainment.SetCInverted
import auth.datalab.sequenceDetection.{Structs, TraceGenerator, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object SetcontainmentBigData {
  case class SetCInverted(event: String, ids: List[Long])

  private var cassandraConnection: CassandraSetContainment = null

  def main(args: Array[String]): Unit = {
    val fileName: String = args(0)
    val type_of_algorithm = args(1) //parsing, indexing or state
    val deleteAll = args(2)
    val join = args(3).toInt
    val deletePrevious = args(4)
    println(fileName, type_of_algorithm, deleteAll, join)
    //    var logName = fileName.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    var logName = fileName.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    Logger.getLogger("org").setLevel(Level.ERROR)

    cassandraConnection = new CassandraSetContainment()
    cassandraConnection.startSpark()
    if (deletePrevious == "1" || deleteAll == "1") {
      cassandraConnection.dropTable(logName)
    }
    cassandraConnection.createTable(logName)

    try {
      val spark = SparkSession.builder().getOrCreate()
      val traceGenerator: TraceGenerator = new TraceGenerator(args(6).toInt, args(7).toInt, args(8).toInt, args(9).toInt)
      val allExecutors = spark.sparkContext.getExecutorMemoryStatus.keys.size
      val minExecutorMemory = spark.sparkContext.getExecutorMemoryStatus.map(_._2._1).min
      println(s"Number of executors= $allExecutors, with minimum memory=$minExecutorMemory")
      val traces = traceGenerator.numberOfTraces
      val size_estimate_trace: scala.math.BigInt = SizeEstimator.estimate(traceGenerator.estimate_size().events.head) * traceGenerator.maxTraceSize
      var partitionNumber = if (minExecutorMemory >= size_estimate_trace * traces) 0 else ((size_estimate_trace * traces) / minExecutorMemory).toInt + 1
      partitionNumber = partitionNumber / allExecutors + 2
      val ids = (1 to traces).toList.sliding((traces / partitionNumber), (traces / partitionNumber).toInt).toList
      println("Iterations: ", ids.length)
      var k = 0L
      for (id <- ids) {
        val sequencesRDD: RDD[Structs.Sequence] = traceGenerator.produce(id)
          .repartition(allExecutors)

        val start = System.currentTimeMillis()
        val inverted_index = sequencesRDD.flatMap(x => {
          val id = x.sequence_id
          x.events.map(_.event).distinct.map(y => (y, id))
        })
          .distinct
          .groupBy(_._1)
          .map(x => {
            val sorted = x._2.toList.map(_._2).distinct.sortWith((a, b) => a < b)
            SetContainment.SetCInverted(x._1, sorted)
          })
        cassandraConnection.writeTableSequenceIndex(inverted_index, logName)
        cassandraConnection.writeTableSeq(sequencesRDD, logName)
        inverted_index.unpersist()
        sequencesRDD.unpersist()
        k = k + System.currentTimeMillis() - start
      }

      println(s"Time taken: ${k} ms")
      cassandraConnection.closeSpark()

      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println("ALL RESULTS IN MB")
      println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      println("** Free Memory:  " + runtime.freeMemory / mb)
      println("** Total Memory: " + runtime.totalMemory / mb)
      println("** Max Memory:   " + runtime.maxMemory / mb)
    } catch {
      case e: Exception => {
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()
      }
    }
  }
}
