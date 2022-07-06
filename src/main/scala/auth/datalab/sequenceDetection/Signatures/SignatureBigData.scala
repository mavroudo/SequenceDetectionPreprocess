package auth.datalab.sequenceDetection.Signatures


import auth.datalab.sequenceDetection.{Structs, TraceGenerator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

object SignatureBigData {

  def main(args: Array[String]): Unit = {
    //    val fileName: String = args(0)
    //    val type_of_algorithm = args(1) //parsing, indexing or state
    //    val deleteAll = args(2)
    //    val join = args(3).toInt
    //    val deletePrevious = args(4)
    //    var k = 10
    //    println(fileName, type_of_algorithm, deleteAll, join)
    //    //    var logName = fileName.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    //    var logName = fileName.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    //    Logger.getLogger("org").setLevel(Level.ERROR)
    //
    //    val cassandraConnection = new CassandraConnection()
    //    cassandraConnection.startSpark()
    //    if (deletePrevious == "1" || deleteAll == "1") {
    //      cassandraConnection.dropTables(logName)
    //    }
    //    cassandraConnection.createTables(logName)
    //    try {
    //      val spark = SparkSession.builder.getOrCreate()
    //      val traceGenerator: TraceGenerator = new TraceGenerator(args(6).toInt, args(7).toInt, args(8).toInt, args(9).toInt)
    //      val allExecutors = spark.sparkContext.getExecutorMemoryStatus.keys.size
    //      val minExecutorMemory = spark.sparkContext.getExecutorMemoryStatus.map(_._2._1).min
    //      println(s"Number of executors= $allExecutors, with minimum memory=$minExecutorMemory")
    //      val traces = traceGenerator.numberOfTraces
    //      val size_estimate_trace: scala.math.BigInt = SizeEstimator.estimate(traceGenerator.estimate_size().events.head) * traceGenerator.maxTraceSize
    //      var partitionNumber = if (minExecutorMemory >= size_estimate_trace * traces) 0 else ((size_estimate_trace * traces) / minExecutorMemory).toInt + 1
    //      partitionNumber = partitionNumber / allExecutors + 2
    //      val ids = (1 to traces).toList.sliding(5000, 5000).toList
    //      println("Iterations: ", ids.length)
    //
    //      val startRDD = traceGenerator.produce((1 to 1000).toList)
    //      val topKfreqPairs: List[(String, String)] = startRDD.map(createPairs).flatMap(x => x.pairs)
    //        .map(x => ((x.eventA, x.eventB), 1))
    //        .reduceByKey(_ + _)
    //        .sortBy(_._2, false)
    //        .take(k)
    //        .map(_._1)
    //        .toList
    //      val events: List[String] = startRDD.flatMap(_.events).map(_.event).distinct().collect.toList
    //      cassandraConnection.writeTableMetadata(events, topKfreqPairs, logName)
    //      println("Metadata saved ...")
    //      val bPairs = spark.sparkContext.broadcast(topKfreqPairs)
    //      val bEvents = spark.sparkContext.broadcast(events)
    //      var t = 0L
    //      for (id <- ids) {
    //        println(id.head, id.last)
    //        val sequencesRDD: RDD[Structs.Sequence] = traceGenerator.produce(id)
    //          .repartition(allExecutors)
    //        val start = System.currentTimeMillis()
    //        k = sequencesRDD.flatMap(x => x.events).map(_.event).distinct.count().toInt
    //        cassandraConnection.writeTableSeq(sequencesRDD, logName)
    //        val signatures = sequencesRDD.map(x => createBSignature(x, bEvents, bPairs))
    //          .groupBy(_.signature)
    //          .map(x => Signatures(x._1, x._2.map(_.sequence_id.toString).toList))
    //
    //        signatures.persist(StorageLevel.MEMORY_AND_DISK)
    //        cassandraConnection.writeTableSign(signatures, logName)
    //        sequencesRDD.unpersist()
    //        signatures.unpersist()
    //        t = t + System.currentTimeMillis() - start
    //
    //      }
    //      println(s"Time taken: ${t} ms")
    //      cassandraConnection.closeSpark()
    //
    //      val mb = 1024 * 1024
    //      val runtime = Runtime.getRuntime
    //      println("ALL RESULTS IN MB")
    //      println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    //      println("** Free Memory:  " + runtime.freeMemory / mb)
    //      println("** Total Memory: " + runtime.totalMemory / mb)
    //      println("** Max Memory:   " + runtime.maxMemory / mb)
    //    } catch {
    //      case e: Exception => {
    //        e.getStackTrace.foreach(println)
    //        println(e.getMessage)
    //        cassandraConnection.closeSpark()
    //      }
    //    }
    //
    //  }
    //
    //  def createBSignature(sequence: Structs.Sequence, events: Broadcast[List[String]], topKfreqPairs: Broadcast[List[(String, String)]]): Signature = {
    //    val s = new mutable.StringBuilder()
    //    val containedEvents = sequence.events.map(_.event).distinct
    //    for (event <- events.value) {
    //      if (containedEvents.contains(event)) s += '1' else s += '0'
    //    }
    //    val pairs = createPairs(sequence)
    //    for (freqPair <- topKfreqPairs.value) {
    //      if (pairs.pairs.map(x => (x.eventA, x.eventB)).contains(freqPair)) s += '1' else s += '0'
    //    }
    //    new Signature(s.toString(), sequence.sequence_id)
    //  }
  }
}
