package auth.datalab.siesta.BusinessLogic.IngestData

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.DetailedSequence
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.TraceGenerator.TraceGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.util.Try

/**
 * Creates the RDD containing the traces either based on an input file or based on randomly generated traces
 */
object IngestingProcess {

  /**
   * This methods creates the RDD that contains the traces. These traces are either parsed from a log file or generated
   * based on the user define parameters in configuration file.
   *
   * @param c The configuration object containing the command line parameters. Note that in order to activate the
   *          trace generator, the "filename" parameter in the configuration object must be set to "synthetic"
   * @return The RDD that contains the traces
   * @see [[ReadLogFile]],[[auth.datalab.siesta.TraceGenerator.TraceGenerator]] they are responsible for parsing a logfile
   *      or generate random traces respectively.
   */
  def getData(c: Config): RDD[Structs.Sequence] = {
    if (c.filename != "synthetic") {
      ReadLogFile.readLog(c.filename)
    } else {
      val traceGenerator = new TraceGenerator(c.traces, c.event_types, c.length_min, c.length_max)
      traceGenerator.produce((1 to c.traces).toList)
    }
  }

  def getDataDetailed(c: Config): RDD[Structs.DetailedSequence] = {
    // Parse the logged data
    var detailedSequenceRDD: RDD[Structs.DetailedSequence] = ReadLogFile.readLogDetailed(c.filename)
    // Find all the event types captured
    val eventTypes: Array[Structs.EventType] = detailedSequenceRDD.flatMap(_.events.map(_.event_type)).distinct().collect()
    // Find all the resources captured
    val resources: List[(String, List[Structs.DetailedEvent])] = detailedSequenceRDD
      .flatMap(_.events.map(event => (event.resource, event)))
      .groupBy(_._1)
      .mapValues(_.map(_._2).toList)
      .collect()
      .toList
    // Find all 2-sets of events captured in direct-following order in some trace
    val pairs: Map[(Structs.EventType, Structs.EventType), Long] = detailedSequenceRDD.map(seq => createEventPairs(seq.events))
      .collect()
      .flatten
      .map { case (key1, key2, value) => ((key1, key2), value) }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
    // Find all concurrent events in some trace
    val concurrentEventPairs: Set[(Structs.EventType, Structs.EventType)] = findConcurrency(detailedSequenceRDD, pairs, eventTypes)
    // Determine enablement time (temporarily as start time) for every event
    detailedSequenceRDD = determineEnablementTime(detailedSequenceRDD, concurrentEventPairs)
    // Determine start time for every event
    detailedSequenceRDD = determineStartTime(detailedSequenceRDD, resources)
    detailedSequenceRDD
  }

  private def createEventPairs(events: List[Structs.DetailedEvent]): Array[(Structs.EventType, Structs.EventType, Long)] = {
    val pairCounter = Map[(Structs.EventType, Structs.EventType), Int]().withDefaultValue(0)
    val eventPairs = events.sliding(2).collect {
      case List(event1, event2) =>
        val pair = (event1.event_type, event2.event_type)
        pairCounter(pair) += 1
        (event1.event_type, event2.event_type, pairCounter(pair))
    }
    eventPairs.toArray
  }

  /**
   * We define concurrency of two activity instances (a.k.a. tasks) as described in the paper "Split Miner:
   * Discovering Accurate and Simple Business Process Models from Event Logs" (Augusto et al. 2017, https://kodu.ut.ee/~dumas/pubs/icdm2017-split-miner.pdf).
   * Tasks A and B are concurrent iff:
   *  - there are 2 traces in log L such that in one trace A is directly followed by B, and in the other trace B is directly followed by A.
   *  - there is no trace in log L such that A is directly followed by B and B is directly followed by A.
   *  - the ratio (| |A->B| - |B->A| |) / (|A->B| + |B->A|) is less than 1.
   */
  private def findConcurrency(traces: RDD[Structs.DetailedSequence], pairs: Map[(Structs.EventType, Structs.EventType), Long], types: Array[Structs.EventType]): Set[(Structs.EventType, Structs.EventType)] = {
    var nonConcurrents = Set[(Structs.EventType, Structs.EventType)]()
    var candidates = Set[(Structs.EventType, Structs.EventType)]()

    // Find all candidate concurrent activity pairs
    // O(n^2) but since n is small, it's ok
    for (i <- types; j <- types) {
      if (i != j) {
        candidates += (i, j)
      }
    }

    // Exonerate certain non-concurrent pairs based on the first condition
    // O(n) but since n is small, it's ok
    for (candidate <- candidates) {
      if (!pairs.contains(candidate) || pairs(candidate) == 0) {
        nonConcurrents += candidate
        nonConcurrents += (candidate._2, candidate._1)
      }
    }

    // Exonerate certain non-concurrent pairs based on the second condition
    traces.flatMap(findNoSelfLoopsTriplets).foreach { case (x, y, z) =>
      nonConcurrents += (x.event_type, y.event_type)
      nonConcurrents += (y.event_type, x.event_type)
    }

    // Clear the candidates set from the non-concurrent pairs
    candidates --= nonConcurrents

    // Exonerate certain non-concurrent pairs based on the third condition
    for (candidate <- candidates) {
      val candidateReversed = (candidate._2, candidate._1)
      if (Math.abs(pairs(candidate) - pairs(candidateReversed)).toFloat / (pairs(candidate) + pairs(candidateReversed)).toFloat >= 1) {
        nonConcurrents += candidate
        nonConcurrents += candidateReversed
      }
    }

    // Final cleaning the candidates set from the non-concurrent pairs
    candidates --= nonConcurrents

    candidates
  }

  private def findNoSelfLoopsTriplets(sequence: Structs.DetailedSequence): List[(Structs.DetailedEvent, Structs.DetailedEvent, Structs.DetailedEvent)] = {
    val events = sequence.events
    val n = events.length

    val triplets = for {
      i <- 0 until n - 2
      x = events(i)
      y = events(i + 1)
      z = events(i + 2)
      if x.event_type == z.event_type && y != x
    } yield (x, y, z)

    triplets.toList
  }

  private def determineEnablementTime(sequences: RDD[Structs.DetailedSequence], concurrents: Set[(Structs.EventType, Structs.EventType)]): RDD[Structs.DetailedSequence] = {
    /*  Start time policy for the first task of a trace:
     *   - If the first task's end time is not at the exact hour, then the start time is the exact hour.
     *   - If the first task's end time is at the exact hour, then the start time is the exact hour minus one hour.
     */
    sequences.map { sequence =>
      sequence.events.zipWithIndex.map { case (event, index) =>
        val endTimestamp = LocalDateTime.parse(event.end_timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val startTimestamp = if (index == 0) {
          if (endTimestamp.getMinute != 0 || endTimestamp.getSecond != 0) {
            endTimestamp.withMinute(0).withSecond(0).withNano(0)
          } else if (endTimestamp.getHour == 0) {
            endTimestamp.withHour(23).withMinute(0).withSecond(0).withNano(0)
          } else {
            endTimestamp.withHour(endTimestamp.getHour - 1).withMinute(0)
          }
        } else {
          val previousEvent = sequence.events(index - 1)
          val previousEndTimestamp = LocalDateTime.parse(previousEvent.end_timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          if (concurrents.contains((previousEvent.event_type, event.event_type))) {
            previousEndTimestamp
          } else {
            previousEvent.start_timestamp
          }
        }
        event.start_timestamp = startTimestamp.toString
        event
      }
      sequence
    }
  }

  private def determineStartTime(sequences: RDD[Structs.DetailedSequence], resources: List[(String, List[Structs.DetailedEvent])]): RDD[Structs.DetailedSequence] = {
    /*  Fix the start time of the tasks based on the resource availability.
     *  The start time of the task is defined as the maximum of: the enablement time of the task,
     *  and the end time of the last task of the same resource which is previous to its end time.
     */
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    sequences.map { sequence =>
      sequence.events.map { event =>
        val available_times = resources.filter(_._1 == event.resource)
          .flatMap(_._2) // Keep the List of events using the current resource
          .filter(event => Instant.parse(event.end_timestamp).isBefore(Instant.parse(event.end_timestamp)))

        if (available_times.nonEmpty) {
          val maxTime = available_times.maxBy(event => Instant.parse(event.end_timestamp))
          val maxTimeInstant = Instant.parse(maxTime.end_timestamp)
          val eventStartInstant = Instant.parse(event.start_timestamp)

          if (eventStartInstant.isBefore(maxTimeInstant)) {
            val waitingTime = Math.abs(maxTimeInstant.getEpochSecond - eventStartInstant.getEpochSecond)
            event.waiting_time = waitingTime
            event.start_timestamp = maxTime.end_timestamp
          }
        }
        event
      }
      sequence
    }
  }

}
