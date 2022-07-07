package auth.datalab.sequenceDetection.Signatures

import auth.datalab.sequenceDetection.CommandLineParser.{Config, Utilities}
import auth.datalab.sequenceDetection.CommandLineParser.Utilities.Iterations
import auth.datalab.sequenceDetection.Structs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object Signatures {
  case class Pair(eventA: String, eventB: String)

  case class PairsInSequence(sequence_id: Long, pairs: List[Pair], sequence: Structs.Sequence)

  case class Signature(signature: String, sequence_id: Long)

  case class Signatures(signature: String, sequence_ids: List[String])

  def execute(c: Config): Unit = {
    val logName = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    val cassandraConnection = new CassandraConnection()
    cassandraConnection.startSpark()
    if (c.delete_previous) {
      cassandraConnection.dropTables(logName)
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables(logName)
    try {
      val spark = SparkSession.builder.getOrCreate()
      val init = Utilities.getRDD(c, 1000)
      val data = init.data
      val traces: Int = Utilities.getTraces(c, data)
      val iterations: Iterations = Utilities.getIterations(c, data, traces)
      val ids: List[Array[Long]] = Utilities.getIds(c, data, traces, iterations.iterations)
      val events: List[String] = {
        if (c.filename != "synthetic") {
          data.flatMap(_.events).map(_.event).distinct().collect.toList
        } else {
          init.traceGenerator.getActivities.toList
        }
      }
      val k: Int = if (c.k == -1) events.size else c.k
      val topKfreqPairs = data.map(createPairs).flatMap(x => x.pairs)
        .map(x => ((x.eventA, x.eventB), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(k)
        .map(_._1)
        .toList
      cassandraConnection.writeTableMetadata(events, topKfreqPairs, logName)
      val bPairs = spark.sparkContext.broadcast(topKfreqPairs)
      val bEvents = spark.sparkContext.broadcast(events)
      var time = 0L

      for (id <- ids) {
        val sequencesRDD: RDD[Structs.Sequence] = Utilities.getNextData(c, data, id, init.traceGenerator, iterations.allExecutors)
        val start = System.currentTimeMillis()
        cassandraConnection.writeTableSeq(sequencesRDD, logName)
        val signatures = sequencesRDD.map(x => createBSignature(x, bEvents, bPairs))
          .groupBy(_.signature)
          .map(x => Signatures(x._1, x._2.map(_.sequence_id.toString).toList))
        signatures.persist(StorageLevel.MEMORY_AND_DISK)
        cassandraConnection.writeTableSign(signatures, logName)
        sequencesRDD.unpersist()
        signatures.unpersist()
        time = time + System.currentTimeMillis() - start
      }
      println(s"Time taken: $time ms")
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

    def createPairs(line: Structs.Sequence): PairsInSequence = {
      val l = new mutable.HashSet[Pair]()
      for (i <- line.events.indices) {
        for (j <- i until line.events.size) {
          l += Pair(line.events(i).event, line.events(j).event)
        }
      }
      PairsInSequence(line.sequence_id, l.toList, line)
    }

    def createBSignature(sequence: Structs.Sequence, events: Broadcast[List[String]], topKfreqPairs: Broadcast[List[(String, String)]]): Signature = {
      val s = new mutable.StringBuilder()
      val containedEvents = sequence.events.map(_.event).distinct
      for (event <- events.value) {
        if (containedEvents.contains(event)) s += '1' else s += '0'
      }
      val pairs = createPairs(sequence)
      for (freqPair <- topKfreqPairs.value) {
        if (pairs.pairs.map(x => (x.eventA, x.eventB)).contains(freqPair)) s += '1' else s += '0'
      }
      Signature(s.toString(), sequence.sequence_id)
    }
  }
}
