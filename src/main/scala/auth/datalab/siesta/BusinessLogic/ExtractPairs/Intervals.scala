package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer

object Intervals {

  private def calculateIntervals(metaData: MetaData, minTimestamp: Date, maxTimestamp: Date): List[Structs.Interval] = {
    val buffer:ListBuffer[Structs.Interval] = new ListBuffer[Structs.Interval]()
    val days = metaData.split_every_days
    if(metaData.last_interval==""){
      var nTime=minTimestamp.toInstant.plus(days,ChronoUnit.DAYS)
      var pTime=minTimestamp.toInstant
      buffer+=Structs.Interval(Date.from(pTime),Date.from(nTime))
      while(nTime.isBefore(maxTimestamp.toInstant)){
        pTime=nTime.plus(1,ChronoUnit.DAYS)
        nTime=nTime.plus(days+1,ChronoUnit.DAYS)
        buffer+=Structs.Interval(Date.from(pTime),Date.from(nTime))
      }
    }else{ //we only calculate forward (there should not be any value that belongs to previous interval)
      val timestamps = metaData.last_interval.split("_")
      var start = Instant.parse(timestamps.head)
      var end = Instant.parse(timestamps.last)
      if(minTimestamp.toInstant.isBefore(start)){
        Logger.getLogger("Calculating intervals").log(Level.ERROR,s"There is an event that has timestamp before the last interval")
        System.exit(12)
      }
      while(end.isBefore(maxTimestamp.toInstant)){
        start=end.plus(1,ChronoUnit.DAYS)
        end=end.plus(days+1,ChronoUnit.DAYS)
        buffer+=Structs.Interval(Date.from(start),Date.from(end))
      }
    }
    buffer.toList
    Logger.getLogger("Calculate Intervals").log(Level.INFO,s"found ${buffer.size} intervals.")
  }

  def intervals(sequenceRDD:RDD[Structs.Sequence],metaData: MetaData):List[Structs.Interval]={
    implicit def ordered:Ordering[Timestamp] = new Ordering[Timestamp] {
      override def compare(x: Timestamp, y: Timestamp): Int = {
        x compareTo y
      }
    }
    val min = sequenceRDD.map(x=>Timestamp.valueOf(x.events.head.timestamp)).min()
    val max = sequenceRDD.map(x=>Timestamp.valueOf(x.events.last.timestamp)).max()
    this.calculateIntervals(metaData,min,max)
  }


}
