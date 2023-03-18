package auth.datalab.siesta.SetContainment

import auth.datalab.siesta.BusinessLogic.IngestData.IngestingProcess
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.Serializable

object SetContainment {
  case class SetCInverted(event: String, ids: List[Long])

  def execute(c: Config): Unit = {
    val cassandraConnection = new CassandraConnectionSetContainment()
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
      val start = System.currentTimeMillis()
      val inverted_index: RDD[SetCInverted] = sequenceRDD.flatMap(x => {
        val id = x.sequence_id
        x.events.map(_.event).distinct.map(y => (y, id))
      })
        .distinct
        .groupBy(_._1)
        .map(x => {
          val sorted = x._2.toList.map(_._2).distinct.sortWith((a, b) => a < b)
          SetCInverted(x._1, sorted)
        })
//      cassandraConnection.readTableSeq(c.log_name)
      cassandraConnection.writeTableSeq(sequenceRDD, c.log_name)
      cassandraConnection.writeTableSequenceIndex(inverted_index, c.log_name)
      inverted_index.unpersist()
      sequenceRDD.unpersist()
      val time = System.currentTimeMillis() - start

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
  }
}
