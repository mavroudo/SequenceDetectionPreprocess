package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Model.{EventTrait, Structs}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer

/**
 * Intervals are used to divide the IndexTable into smaller segments, where the duration of each segment is equal to the
 * split_every_days parameter (described in [[auth.datalab.siesta.CommandLineParser.Config]]).
 */
object Intervals {

  /**
   * Calculates a list of time intervals based on the last stored interval, the split_every_days parameter as well as
   * the min and max timestamp of the newly arrived events. Then each event type pair will be assigned to one of these
   * intervals based on the timestamp of the second event of the pair.
   *
   * @param last_interval    The last interval stored in the database
   * @param split_every_days The parameter that describes the time window for each interval
   * @param minTimestamp     The minimum timestamp of the newly arrived events
   * @param maxTimestamp     The maximum timestamp of the newly arrived events
   * @return The list with the time intervals that expand through that time period
   */
  private def calculateIntervals(last_interval: String, split_every_days: Int, minTimestamp: Timestamp, maxTimestamp: Timestamp): List[Structs.Interval] = {
    val buffer: ListBuffer[Structs.Interval] = new ListBuffer[Structs.Interval]()
    val days = split_every_days
    if (last_interval == "") {
      var nTime = minTimestamp.toInstant.plus(days, ChronoUnit.DAYS)
      var pTime = minTimestamp.toInstant
      buffer += Structs.Interval(Timestamp.from(pTime), Timestamp.from(nTime))
      while (nTime.isBefore(maxTimestamp.toInstant)) {
        pTime = nTime.plus(1, ChronoUnit.DAYS)
        nTime = nTime.plus(days + 1, ChronoUnit.DAYS)
        buffer += Structs.Interval(Timestamp.from(pTime), Timestamp.from(nTime))
      }
    } else { //we only calculate forward (there should not be any value that belongs to previous interval)
      val timestamps = last_interval.split("_")
      var start = Timestamp.valueOf(timestamps.head).toInstant
      var end = Timestamp.valueOf(timestamps.last).toInstant
      //      if(minTimestamp.before(Timestamp.valueOf(start.toString))){
      if (start.isAfter(minTimestamp.toInstant)) { //we do not allow for events with timestamp before the last interval
        Logger.getLogger("Calculating intervals").log(Level.ERROR, s"There is an event that has timestamp before the last interval")
        System.exit(12)
      }
      buffer += Structs.Interval(Timestamp.from(start), Timestamp.from(end))
      while (end.isBefore(maxTimestamp.toInstant)) {
        start = end.plus(1, ChronoUnit.DAYS)
        end = end.plus(days + 1, ChronoUnit.DAYS)
        buffer += Structs.Interval(Timestamp.from(start), Timestamp.from(end))
      }
    }

    Logger.getLogger("Calculate Intervals").log(Level.INFO, s"found ${buffer.size} intervals.")
    val c = buffer.toList.map(x => s"${x.start.toString}_${x.end.toString}").mkString("||")
    Logger.getLogger("Calculate Intervals").log(Level.INFO, s"intervals: $c")
    buffer.toList
  }

  /**
   * The public method that returns the intervals based on a given RDD that contains the traces. The process first
   * calculates the minimum and maximum timestamp of the newly arrived events and then calls the calculateIntervals
   * method.
   *
   * @param sequenceRDD      The RDD that contains the newly arrived traces
   * @param last_interval    The last interval stored in the database
   * @param split_every_days The parameter that describes the time window for each interval
   * @return The list with the time intervals
   */
  def intervals(sequenceRDD: RDD[EventTrait], last_interval: String, split_every_days: Int, min_ts: Timestamp,
                max_ts: Timestamp): List[Structs.Interval] = {

    this.calculateIntervals(last_interval, split_every_days, min_ts, max_ts)
  }


}
