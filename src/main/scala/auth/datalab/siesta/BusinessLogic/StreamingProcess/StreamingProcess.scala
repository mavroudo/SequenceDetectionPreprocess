package auth.datalab.siesta.BusinessLogic.StreamingProcess

import auth.datalab.siesta.BusinessLogic.Model.{EventStream, Structs}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp
import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.collection.mutable

object StreamingProcess {


  case class CustomState(number_of_events: Int, events: mutable.HashMap[String, Array[(Timestamp, Int)]]) extends Serializable

  case class CountState(sum_duration:Long,count:Int,min_duration:Long,max_duration:Long,sum_squares:Double) extends Serializable

  case class LastEventState(lastEvent:mutable.HashMap[String,Timestamp])

  implicit val stateEncoder: Encoder[CustomState] = Encoders.kryo[CustomState]

  implicit  val countStateEncoder: Encoder[CountState] = Encoders.kryo[CountState]

  def createPair(eva:(String,Timestamp,Int),evb:(EventStream,Int)):Structs.StreamingPair={
    val ts_b=Timestamp.valueOf(evb._1.timestamp)
    Structs.StreamingPair(eva._1, evb._1.event_type, evb._1.trace,eva._2, ts_b, eva._3, evb._2)
  }

  def createPair(eva:(String,Timestamp,Int),evb:(String,Timestamp,Int),traceId:String):Structs.StreamingPair={
    Structs.StreamingPair(eva._1, evb._1, traceId,eva._2, evb._2, eva._3, evb._3)
  }

  /**
   * Function that calculates the pairs of the events in an online manner
   * @param traceId The id of the trace
   * @param eventStream Events in the last batch that correspond to this trace
   * @param groupState A State object for each trace that keeps the previous events
   * @return An Iterator with all the generated pairs from this batch
   */
  def calculatePairs(traceId: String, eventStream: Iterator[EventStream], groupState: GroupState[CustomState],lookback:Int): Iterator[Structs.StreamingPair] = {

    //TODO: handle lookback, mode (positions/timestamps)


    val values = eventStream.toSeq
    val initialState = new CustomState(0, new mutable.HashMap[String, Array[(Timestamp, Int)]])
    val oldState = groupState.getOption.getOrElse(initialState)

    val alreadyStoredEvents: Int = oldState.number_of_events
    val newEventsWithIndex = values.zip(Iterator.range(alreadyStoredEvents, alreadyStoredEvents + values.size).toSeq)
    val generatedPairs = mutable.ArrayBuffer[Structs.StreamingPair]()

    val lookbackMillis = Duration.ofDays(lookback).toMillis

    for (evb <- newEventsWithIndex) {
      //First handle the pair of the same event type - and define the previous event of the same type.
      //It is null if this is the first time we find this pair
      val same_type_events: Array[(Timestamp, Int)] = oldState.events.getOrElse(evb._1.event_type, Array.empty[(Timestamp, Int)])
      var prev_event: (String, Timestamp, Int) = null
      if (same_type_events.isEmpty) {
        oldState.events(evb._1.event_type) = Array((Timestamp.valueOf(evb._1.timestamp), evb._2))
      } else {
        val ts_b: Timestamp = Timestamp.valueOf(evb._1.timestamp)
        if (same_type_events.length == 1) { //there is only one event -> append the new one
          //get the previous event
          prev_event = (evb._1.event_type, same_type_events.head._1, same_type_events.head._2)
          oldState.events(evb._1.event_type) = oldState.events(evb._1.event_type) :+ (Timestamp.valueOf(evb._1.timestamp), evb._2)
        } else { //there are two events, replace the second one
          prev_event = (evb._1.event_type, same_type_events(1)._1, same_type_events(1)._2)
          val ts_b: Timestamp = Timestamp.valueOf(evb._1.timestamp)
          oldState.events(evb._1.event_type)(1) = (ts_b, evb._2)
        }
        if (ts_b.getTime - prev_event._2.getTime <= lookbackMillis) {
          val event = StreamingProcess.createPair(prev_event, evb)
          generatedPairs.append(event)
        }
      }

      // Create the pairs (a,b) for different event types. For each event will go through each other type and starting
      // from the first event of that type will generate a pair if this event is between the prev event and the new event.
      // if the prev event is null it will generate pair if the event is before that timestamp of the new event.

      // that is the first appearance of this event type, create pairs with every first event of the other keys
      val evb_transformed = (evb._1.event_type, Timestamp.valueOf(evb._1.timestamp), evb._2)
      val keys = oldState.events.keys.toSeq
      if (prev_event == null) {
        for (key <- keys) {
          if (key != evb._1.event_type) { //for all the other keys
            var i = 0
            while (i < oldState.events(key).length){
              val eva = oldState.events(key)(i)
              val eva_transformed = (key, eva._1, eva._2)
              val diff = evb_transformed._2.getTime - eva_transformed._2.getTime
              if (diff >= 0 && diff < lookbackMillis){
                generatedPairs.append(createPair(eva_transformed, evb_transformed, traceId.toString))
                i = oldState.events(key).length
              }
              else {
                i += 1
              }
            }

          }
        }
      } else { //there is a previous event stored
        // Have to find the event for each different type that is the first one between prev_event and evb
        for (key <- keys) {
          if (key != evb._1.event_type) { //for all the other keys
            var i = 0
            while (i < oldState.events(key).length) { //check each of the max 2 events one that match the constraints
              val eva = oldState.events(key)(i)
              if (eva._2 > prev_event._3 && eva._2 < evb._2) {
                val eva_transformed = (key, eva._1, eva._2)
                val diff = evb_transformed._2.getTime - eva_transformed._2.getTime
                if (diff >= 0 && diff < lookbackMillis){
                  generatedPairs.append(createPair(eva_transformed, evb_transformed, traceId.toString))
                  i = oldState.events(key).length
                }
                else {
                  i += 1;
                }
              } else {
                i += 1;
              }
            }
          }
        }
      }
    }
    groupState.setTimeoutTimestamp(groupState.getCurrentWatermarkMs(), s"$lookback day")
    //Update the previous state with the new one
    val newState = new CustomState(oldState.number_of_events + values.size, oldState.events)
    groupState.update(newState)
    // return the pairs
    generatedPairs.iterator
  }

  def calculateMetrics(eventPair:(String,String), c:Iterator[Structs.Count],groupState:GroupState[CountState]):Iterator[Structs.Count]={
    val values = c.toSeq
    val initialState = new CountState(0, 0,Long.MaxValue,0,0)
    val oldState = groupState.getOption.getOrElse(initialState)

    val sum_durations =values.map(_.sum_duration).sum + oldState.sum_duration
    val sum_square =  values.map(_.sum_squares).sum + oldState.sum_squares
    val min_duration = Math.min(values.map(_.min_duration).min,oldState.min_duration)
    val max_duration = Math.max(values.map(_.max_duration).max,oldState.max_duration)
    val counts = values.size+oldState.count

    val newState = CountState(sum_durations,counts,min_duration, max_duration,sum_square)
    groupState.update(newState)

    Iterator(Structs.Count(eventPair._1,eventPair._2,sum_durations,counts,min_duration, max_duration,sum_square))
  }


  /**
   * Maintains the last event of each trace in order to remove late arriving events
   * @param traceId The id of the trace
   * @param eventStream The events in the last batch
   * @param groupState The state that maintains the last event per trace
   * @return The events that occur after the last stored event in this trace
   */
  def filterEvents(traceId:String, eventStream:Iterator[EventStream],groupState: GroupState[LastEventState]):Iterator[EventStream]={
    val values = eventStream.toSeq
    val initialState = new LastEventState(new mutable.HashMap[String,Timestamp]())
    val oldState = groupState.getOption.getOrElse(initialState)

    val lastEvent = oldState.lastEvent.getOrElse(traceId,null)
    if (lastEvent==null){ //there is no previous event in this trace
      oldState.lastEvent(traceId)=Timestamp.valueOf(values.last.timestamp)
      groupState.update(oldState)
      eventStream
    }else{ // there are already data in this trace
      val filtered = values.filter(x=>Timestamp.valueOf(x.timestamp).after(lastEvent))
      if(filtered.nonEmpty){
        oldState.lastEvent(traceId)=Timestamp.valueOf(filtered.last.timestamp)
        groupState.update(oldState)
      }
      filtered.iterator
    }

  }




}