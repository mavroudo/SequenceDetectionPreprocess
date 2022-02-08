package auth.datalab.sequenceDetection.Signatures

import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Signature {
  case class Pair(eventA: String, eventB: String)

  case class PairsInSequence(sequence_id: Long, pairs: List[Pair], sequence: Structs.Sequence)

  case class Signature(signature: String, sequence_id: Long)

  case class Signatures(signature: String, sequence_ids: List[String])

  def main(args: Array[String]): Unit = {
    val fileName: String = args(0)
    val type_of_algorithm = args(1) //parsing, indexing or state
    val deleteAll = args(2)
    val join = args(3).toInt
    val deletePrevious = args(4)
    val k = 10
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
    val spark = SparkSession.builder.getOrCreate()
    spark.time({
      val sequencesRDD: RDD[Structs.Sequence] = Utils.readLog(fileName)
        .persist(StorageLevel.MEMORY_AND_DISK)
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
      signatures.take(2).foreach(println)
      cassandraConnection.writeTableSign(signatures, logName)
      cassandraConnection.writeTableMetadata(events, topKfreqPairs, logName)
      signatures.unpersist()
    })
    cassandraConnection.closeSpark()
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("ALL RESULTS IN MB")
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
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

  def createSignature(sequence: Structs.Sequence, events: List[String], topKfreqPairs: List[(String, String)]): Signature = {
    val signature = new StringBuilder()
    val containedEvents = sequence.events.map(_.event).distinct
    for (event <- events) {
      if (containedEvents.contains(event)) signature += '1' else signature += '0'
    }
    val pairs = createPairs(sequence)
    for (freqPair <- topKfreqPairs) {
      if (pairs.pairs.map(x => (x.eventA, x.eventB)).contains(freqPair)) signature += '1' else signature += '0'
    }
    Signature(signature.toString(), sequence.sequence_id)
  }
}
