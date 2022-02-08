package auth.datalab.sequenceDetection.SetContainment
import auth.datalab.sequenceDetection.PairExtraction.{Indexing, Parsing, SkipTillAnyMatch, State, StrictContiguity}
import auth.datalab.sequenceDetection.{Structs, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SetContainment {
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
      spark.time({
        val sequencesRDD: RDD[Structs.Sequence] = Utils.readLog(fileName)
        sequencesRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val inverted_index = sequencesRDD.flatMap(x=>{
          val id=x.sequence_id
          x.events.map(_.event).distinct.map(y=>(y,id))
        })
          .distinct
          .groupBy(_._1)
          .map(x=>{
            val sorted = x._2.toList.map(_._2).distinct.sortWith((a,b)=>a<b)
            SetCInverted(x._1,sorted)
          })
        cassandraConnection.writeTableSequenceIndex(inverted_index, logName)
        cassandraConnection.writeTableSeq(sequencesRDD, logName)
        inverted_index.unpersist()
        sequencesRDD.unpersist()
        cassandraConnection.closeSpark()
      })
      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println("ALL RESULTS IN MB")
      println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      println("** Free Memory:  " + runtime.freeMemory / mb)
      println("** Total Memory: " + runtime.totalMemory / mb)
      println("** Max Memory:   " + runtime.maxMemory / mb)
    }catch {
      case e: Exception => {
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()
      }
    }
  }


}