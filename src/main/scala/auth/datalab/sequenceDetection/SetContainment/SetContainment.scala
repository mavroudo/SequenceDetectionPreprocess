package auth.datalab.sequenceDetection.SetContainment

import auth.datalab.sequenceDetection.CommandLineParser.Utilities.Iterations
import auth.datalab.sequenceDetection.CommandLineParser.{Config, Utilities}
import auth.datalab.sequenceDetection.Structs
import auth.datalab.sequenceDetection.Structs.SetCInverted
import org.apache.spark.rdd.RDD


object SetContainment {

  private var cassandraConnection: CassandraSetContainment = _

  def execute(c: Config): Unit = {
    val logName = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    cassandraConnection = new CassandraSetContainment()
    cassandraConnection.startSpark()
    if (c.delete_previous) {
      cassandraConnection.dropTables(logName)
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables(logName)


    try {
      val init = Utilities.getRDD(c, 100)
      val sequencesRDD_before_repartitioned = init.data
      val traces: Int = Utilities.getTraces(c, sequencesRDD_before_repartitioned)
      val iterations: Iterations = Utilities.getIterations(c, sequencesRDD_before_repartitioned, traces)
      val ids: List[Array[Long]] = Utilities.getIds(c, sequencesRDD_before_repartitioned, traces, iterations.iterations)
      var k = 0L
      for (id <- ids) {
        val start = System.currentTimeMillis()
        val sequencesRDD: RDD[Structs.Sequence] = Utilities.getNextData(c, sequencesRDD_before_repartitioned, id, init.traceGenerator, iterations.allExecutors)
        val inverted_index:RDD[SetCInverted] = sequencesRDD.flatMap(x=>{
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
        k = k + System.currentTimeMillis() - start
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
