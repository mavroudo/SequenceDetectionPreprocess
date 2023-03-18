package auth.datalab.siesta.Singatures

import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
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
    //    val logName = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    val cassandraConnection = new CassandraConnectionSignatures()
    cassandraConnection.startSpark()
    if (c.delete_previous) {
      cassandraConnection.dropTables(c.log_name)
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables(c.log_name)
    try {
      val spark = SparkSession.builder.getOrCreate()
      val sequenceRDD: RDD[Structs.Sequence] = IngestingProcess.getData(c)

      val metadata = loadOrCreateSignature(c, sequenceRDD)
      val bPairs = spark.sparkContext.broadcast(metadata._2)
      val bEvents = spark.sparkContext.broadcast(metadata._1)
      var time = 0L

      val start = System.currentTimeMillis()
      cassandraConnection.writeTableSeq(sequenceRDD, c.log_name)
      val combined:RDD[Structs.Sequence] = cassandraConnection.readTableSeq(c.log_name)
      val signatures = combined.map(x => createBSignature(x, bEvents, bPairs))
        .groupBy(_.signature)
        .map(x => Signatures(x._1, x._2.map(_.sequence_id.toString).toList))
      signatures.persist(StorageLevel.MEMORY_AND_DISK)
      cassandraConnection.writeTableSign(signatures, c.log_name)
      sequenceRDD.unpersist()
      signatures.unpersist()
      time = time + System.currentTimeMillis() - start

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

    def loadOrCreateSignature(c: Config, sequenceRDD: RDD[Structs.Sequence]): (List[String], List[(String, String)]) = {
      val df = cassandraConnection.loadTableMetadata(c.log_name)
      if (df.count() == 0) {
        val events: List[String] = sequenceRDD.flatMap(_.events).map(_.event).distinct().collect().toList
        val k: Int = if (c.k == -1) events.size else c.k
        val topKfreqPairs = sequenceRDD.map(createPairs).flatMap(x => x.pairs)
          .map(x => ((x.eventA, x.eventB), 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, ascending = false)
          .take(k)
          .map(_._1)
          .toList
        cassandraConnection.writeTableMetadata(events, topKfreqPairs, c.log_name)
        (events, topKfreqPairs)
      } else {
        val pairs:List[(String,String)] = df.filter(row=>row.getAs[String]("object")=="pairs")
          .head().getSeq[String](1).toList.map(x=>{
          val s = x.split(",")
          (s(0),s(1))
        })
        val events:List[String] = df.filter(row => row.getAs[String]("object") == "events")
          .head().getSeq[String](1).toList
        (events, pairs)
      }

    }
  }

}
