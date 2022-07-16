package auth.datalab.sequenceDetection.CommandLineParser

import auth.datalab.sequenceDetection.{Structs, TraceGenerator, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object Utilities {
  case class RDDInit(traceGenerator: TraceGenerator, data:RDD[Structs.Sequence])

  /**
   * Initialize the data. In case of a file returns the data in RDD, otherwise
   * generates 50 synthetic data, in order to calculate the iterations in the
   * next step
   * @param c The configuration object
   * @param rand_data Number of random data produced if synthetic
   * @return The data and the trace generator (if synthetic is used, otherwise is null)
   */
  def getRDD(c:Config,rand_data:Int):RDDInit ={
    var traceGenerator: TraceGenerator = null
    val sequencesRDD_before_repartitioned: RDD[Structs.Sequence] = {
      if (c.filename != "synthetic") {
        Utils.readLog(c.filename)
      } else {
        traceGenerator = new TraceGenerator(c.traces, c.event_types, c.length_min, c.length_max)
        traceGenerator.produce((1 to rand_data).toList)
      }
    }
    RDDInit(traceGenerator,sequencesRDD_before_repartitioned)
  }

  /**
   * Return the number of traces in the dataset (synthetic/file)
   * @param c Configuration object
   * @param data Data returned from the [[auth.datalab.sequenceDetection.CommandLineParser.Utilities#getRDD(auth.datalab.sequenceDetection.CommandLineParser.Config)]]
   * @return
   */
  def getTraces(c:Config,data:RDD[Structs.Sequence]): Int = {
    if (c.filename == "synthetic") c.traces else data.count().toInt
  }

  case class Iterations(iterations: Int, allExecutors: Int)
  /**
   * Calculate the iterations based on the available memory and number of executors
   * If the user has define the number of iterations it will return that instead
   * @param c The configuration object
   * @param data Data returned from the [[auth.datalab.sequenceDetection.CommandLineParser.Utilities#getRDD(auth.datalab.sequenceDetection.CommandLineParser.Config)]]
   * @param traces number of traces
   * @return
   */
  def getIterations(c:Config,data:RDD[Structs.Sequence],traces:Int):Iterations={
    val spark = SparkSession.builder().getOrCreate()
    val allExecutors = spark.sparkContext.getExecutorMemoryStatus.keys.size
    val iterations: Int = { // determines iterations
      if (c.iterations != -1) c.iterations
      else {
        val minExecutorMemory = spark.sparkContext.getExecutorMemoryStatus.map(_._2._1).min
        val average_length = data.takeSample(withReplacement = false, 50).map(_.events.size).sum / 50
        val size_estimate_trace: scala.math.BigInt = SizeEstimator.estimate(data.take(1)(0).events.head) * average_length * (average_length / 2)
        val partitionNumber = if (minExecutorMemory / size_estimate_trace > traces) 1 else ((size_estimate_trace * traces) / minExecutorMemory).toInt + 1
        partitionNumber / allExecutors
      }
    }
    Iterations(iterations,allExecutors)
  }

  /**
   * Split the ids based on the provided/calculated iterations
   * @param c The configuration object
   * @param data Data returned from the [[auth.datalab.sequenceDetection.CommandLineParser.Utilities#getRDD(auth.datalab.sequenceDetection.CommandLineParser.Config)]]
   * @param traces number of traces
   * @param iterations number of iterations
   * @return
   */
  def getIds(c:Config,data:RDD[Structs.Sequence],traces:Int,iterations:Int):List[Array[Long]]={
    if (c.filename != "synthetic") {
      data
        .map(_.sequence_id)
        .collect().sortWith((x, y) => x < y)
        .sliding(traces / iterations, traces / iterations)
        .toList
    } else {
      (1L to traces).toArray
        .sliding(traces / iterations, traces / iterations)
        .toList
    }
  }

  /**
   * Returns the data in RDD based on the ids in this iterations
   * @param c The configuration object
   * @param data Data returned from the [[auth.datalab.sequenceDetection.CommandLineParser.Utilities#getRDD(auth.datalab.sequenceDetection.CommandLineParser.Config)]]
   * @param ids Ids in this iteration
   * @param traceGenerator in case of synthetic, otherwise will not be used
   * @param allExecutors number of executors to repartition the data
   * @return
   */
  def getNextData(c:Config,data:RDD[Structs.Sequence],ids:Array[Long],traceGenerator: TraceGenerator,allExecutors:Int):RDD[Structs.Sequence]={
    val spark = SparkSession.builder().getOrCreate()
    if (c.filename == "synthetic") {
      traceGenerator.produce(ids)
        .coalesce(spark.sparkContext.defaultParallelism)
//        .repartition(allExecutors)
    } else {
      data
        .filter(x => x.sequence_id >= ids(0) && x.sequence_id <= ids.last)
        .coalesce(spark.sparkContext.defaultParallelism)
//        .repartition(allExecutors)
    }
  }

}
