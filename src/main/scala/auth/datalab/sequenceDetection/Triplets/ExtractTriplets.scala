package auth.datalab.sequenceDetection.Triplets

import auth.datalab.sequenceDetection.SetContainment.SetContainment.cassandraConnection
import auth.datalab.sequenceDetection.Structs.Triplet
import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.{HashMap, ListBuffer}



object ExtractTriplets {

  private var cassandraConnection: CassandraTriplets = null

  def extract(data: RDD[Structs.Sequence]): RDD[Structs.Triplet] = {
    val spark = SparkSession.builder().getOrCreate()
    val combinations = data
      .flatMap(l => this.indexingMethodTriplets(l))
      .keyBy(l => (l.event1, l.event2, l.event3))
      .reduceByKey((a, b) => {
        val newList = List.concat(a.times, b.times)
        Structs.Triplet(a.event1, a.event2, a.event3, newList)
      })
      .map(_._2)
      .coalesce(spark.sparkContext.defaultParallelism)
    combinations
  }

  private def indexingMethodTriplets(line: Structs.Sequence): List[Triplet] = {
    var mapping = HashMap[String, List[String]]()
    val sequence_id = line.sequence_id
    line.events.foreach(event => { //This will do the mapping
      val oldSequence = mapping.getOrElse(event.event, null)
      if (oldSequence == null) {
        mapping.+=((event.event, List(event.timestamp)))
      } else {
        val newList = oldSequence :+ event.timestamp
        mapping.+=((event.event, newList))
      }
    })
    val triplets = new ListBuffer[Triplet]
    for((k1,v1) <- mapping){
      for((k2,v2) <- mapping){
        for((k3,v3) <- mapping){
          val list = List[Structs.IdTimeList]() :+ this.createTripletsIndexing(v1, v2, v3, sequence_id)
          triplets += Triplet(k1, k2, k3, list)
        }
      }
    }
    val l =triplets.filter(x => {
      x.times.head.times.nonEmpty
    })
    l.toList
  }

  private def createTripletsIndexing(eventAtimes: List[String], eventBtimes: List[String], eventCtimes: List[String], sequenceid: Long): Structs.IdTimeList = {
    var posA = 0
    var posB = 0
    var posC = 0
    var prev = ""

    val sortedA = eventAtimes.sortWith(Utils.compareTimes)
    val sortedB = eventBtimes.sortWith(Utils.compareTimes)
    val sortedC = eventCtimes.sortWith(Utils.compareTimes)

    var response = Structs.IdTimeList(sequenceid, List[String]())

    while (posA < sortedA.size && posB < sortedB.size && posC < sortedC.size) {
      if (Utils.compareTimes(sortedA(posA), sortedB(posB)) && Utils.compareTimes(sortedB(posB), sortedC(posC))) { // goes in if (a b c)
        if (Utils.compareTimes(prev, sortedA(posA))) {
          val newList = response.times :+ sortedA(posA) :+ sortedB(posB) :+ sortedC(posC)
          response = Structs.IdTimeList(response.id, newList)
          prev = sortedC(posC)
          posA += 1
          posB += 1
          posC += 1
        } else {
          posA += 1
        }
      } else {
        if (!Utils.compareTimes(sortedA(posA), sortedB(posB))) {
          posB += 1
        } else if (!Utils.compareTimes(sortedB(posB), sortedC(posC))) {
          posC += 1
        }
      }
    }
    response
  }


  def main(args: Array[String]): Unit = {
    val fileName: String = args(0)
    val type_of_algorithm = args(1) //parsing, indexing or state
    val deleteAll = args(2)
    val join = args(3).toInt
    val deletePrevious = args(4)
    println(fileName, type_of_algorithm, deleteAll, join)
    var logName = fileName.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    Logger.getLogger("org").setLevel(Level.ERROR)

    cassandraConnection = new CassandraTriplets()
    cassandraConnection.startSpark()
    if (deletePrevious == "1" || deleteAll == "1") {
      cassandraConnection.dropTable(logName)
    }
    cassandraConnection.createTable(logName)
    try {
      val spark = SparkSession.builder().getOrCreate()
      spark.time({
        val sequencesRDD_before_repartitioned: RDD[Structs.Sequence] = Utils.readLog(fileName)
        val allExecutors = spark.sparkContext.getExecutorMemoryStatus.keys.size
        val minExecutorMemory = spark.sparkContext.getExecutorMemoryStatus.map(_._2._1).min
        println(s"Number of executors= $allExecutors, with minimum memory=$minExecutorMemory")
        val traces = sequencesRDD_before_repartitioned.count()
        val average_length = sequencesRDD_before_repartitioned.takeSample(false, 50).map(_.events.size).sum / 50
        val size_estimate_trace: scala.math.BigInt = SizeEstimator.estimate(sequencesRDD_before_repartitioned.take(1)(0).events.head) * average_length * (average_length*average_length / 3)
        var partitionNumber = if (minExecutorMemory / size_estimate_trace > traces) 1 else ((size_estimate_trace * traces) / minExecutorMemory).toInt + 1
        partitionNumber=partitionNumber/allExecutors +1
        val ids = sequencesRDD_before_repartitioned.map(_.sequence_id).collect().sortWith((x, y) => x < y).sliding((traces / partitionNumber).toInt, (traces / partitionNumber).toInt).toList
        println("Iterations: ", ids.length)
        for (id <- ids) {
          val sequencesRDD: RDD[Structs.Sequence] = sequencesRDD_before_repartitioned
            .filter(x => x.sequence_id >= id(0) && x.sequence_id <= id.last)
            .repartition(allExecutors)
          sequencesRDD.persist(StorageLevel.MEMORY_AND_DISK)
          cassandraConnection.writeTableSeq(sequencesRDD, logName)
          val combinationsRDD = extract(sequencesRDD)
//          combinationsRDD.persist(StorageLevel.MEMORY_AND_DISK)
          sequencesRDD.unpersist()
          cassandraConnection.writeTableSequenceIndex(combinationsRDD, logName)
          combinationsRDD.unpersist()
        }
      })
    } catch {
      case e: Exception => {
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()
      }

    }
  }

}
