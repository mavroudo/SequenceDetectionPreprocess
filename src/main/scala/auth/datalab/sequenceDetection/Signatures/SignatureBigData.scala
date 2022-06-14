package auth.datalab.sequenceDetection.Signatures

import auth.datalab.sequenceDetection.SetContainment.SetcontainmentBigData.cassandraConnection
import auth.datalab.sequenceDetection.Signatures.Signature.{Signatures, createPairs, createSignature}
import auth.datalab.sequenceDetection.{Structs, TraceGenerator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object SignatureBigData {

  def main(args: Array[String]): Unit = {
    val fileName: String = args(0)
    val type_of_algorithm = args(1) //parsing, indexing or state
    val deleteAll = args(2)
    val join = args(3).toInt
    val deletePrevious = args(4)
    var k = 10
    println(fileName, type_of_algorithm, deleteAll, join)
    //    var logName = fileName.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    var logName = fileName.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    Logger.getLogger("org").setLevel(Level.ERROR)

    val cassandraConnection = new CassandraConnection()
    cassandraConnection.startSpark()
    if (deletePrevious == "1" || deleteAll == "1") {
      cassandraConnection.dropTables(logName)
    }
    cassandraConnection.createTables(logName)
    try {
      val spark = SparkSession.builder.getOrCreate()
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
      var t = 0L
      for (id <- ids) {
        val sequencesRDD: RDD[Structs.Sequence] = traceGenerator.produce(id)
          .repartition(allExecutors)
        val start = System.currentTimeMillis()
        k=sequencesRDD.flatMap(x=>x.events).map(_.event).distinct.count().toInt
        cassandraConnection.writeTableSeq(sequencesRDD, logName)

        val topKfreqPairs = sequencesRDD.map(createPairs).flatMap(x => x.pairs)
          .map(x => ((x.eventA, x.eventB), 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, false)
          .take(k)
          .map(_._1)
          .toList

        val events = sequencesRDD.flatMap(_.events).map(_.event).distinct().collect.toList

        val signatures = sequencesRDD.map(x => createSignature(x, events, topKfreqPairs))
          .groupBy(_.signature)
          .map(x => Signatures(x._1, x._2.map(_.sequence_id.toString).toList))
        sequencesRDD.unpersist()
        signatures.persist(StorageLevel.MEMORY_AND_DISK)
        cassandraConnection.writeTableSign(signatures, logName)
        if(id.contains(1)) {
          cassandraConnection.writeTableMetadata(events, topKfreqPairs, logName)
        }
        signatures.unpersist()
        t = t + System.currentTimeMillis() - start

      }
      println(s"Time taken: ${t} ms")
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
