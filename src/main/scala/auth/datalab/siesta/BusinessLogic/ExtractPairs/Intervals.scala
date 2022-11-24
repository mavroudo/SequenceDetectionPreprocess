package auth.datalab.siesta.BusinessLogic.ExtractPairs

import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.log4j.{Level, Logger}

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer

object Intervals {

  def calculateIntervals(metaData: MetaData, minTimestamp: Date, maxTimestamp: Date): List[Structs.Interval] = {
    val buffer:ListBuffer[Structs.Interval] = new ListBuffer[Structs.Interval]()
    if(metaData.last_interval==""){
      var nTime=minTimestamp.toInstant
      var pTime=minTimestamp.toInstant
      while(nTime.isBefore(maxTimestamp.toInstant)){
        nTime=nTime.plus(30,ChronoUnit.DAYS)
        buffer+=Structs.Interval(Date.from(pTime),Date.from(nTime))
        pTime=nTime
      }
    }else{ //we only calculate forward (there should not be any value that belongs to previous interval)
      val timestamps = metaData.last_interval.split(",")
      var start = Instant.parse(timestamps.head)
      var end = Instant.parse(timestamps.last)
      if(minTimestamp.toInstant.isBefore(start)){
        Logger.getLogger("Calculating intervals").log(Level.ERROR,s"There is an event that has timestamp before the last interval")
        System.exit(12)
      }
      while(end.isBefore(maxTimestamp.toInstant)){
        end=end.plus(30,ChronoUnit.DAYS)
        start=start.plus(30,ChronoUnit.DAYS)
        buffer+=Structs.Interval(Date.from(pTime),Date.from(nTime))
        pTime=nTime
      }


    }
    buffer.toList
  }

  //TODO: find min and max timestamp

}
