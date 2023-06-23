package auth.datalab.siesta.BusinessLogic.StreamingProcess

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp
import scala.collection.mutable

object StreamingProcess {

  case class CustomState(number_of_events: Int, events: mutable.HashMap[String, Array[(Timestamp, Int)]]) extends Serializable

  case class CountState(sum_duration:Long,count:Int,min_duration:Long,max_duration:Long) extends Serializable

  implicit val stateEncoder: Encoder[CustomState] = Encoders.kryo[CustomState]

  implicit  val countStateEncoder: Encoder[CountState] = Encoders.kryo[CountState]

  def createPair(eva:(String,Timestamp,Int),evb:(EventStream,Int)):Structs.StreamingPair={
    Structs.StreamingPair(eva._1, evb._1.event_type, evb._1.trace,eva._2, evb._1.timestamp, eva._3, evb._2)
  }

  def createPair(eva:(String,Timestamp,Int),evb:(String,Timestamp,Int),traceId:Int):Structs.StreamingPair={
    Structs.StreamingPair(eva._1, evb._1, traceId,eva._2, evb._2, eva._3, evb._3)
  }

  /**
   * Function that calculates the pairs of the events in an online manner
   * @param traceId The id of the trace
   * @param eventStream Events in the last batch that correspond to this trace
   * @param groupState A State object for each trace that keeps the previous events
   * @return An Iterator with all the generated pairs from this batch
   */
  def calculatePairs(traceId: Long, eventStream: Iterator[Structs.EventStream], groupState: GroupState[CustomState]): Iterator[Structs.StreamingPair] = {

    val values = eventStream.toSeq
    val initialState = new CustomState(0, new mutable.HashMap[String, Array[(Timestamp, Int)]])
    val oldState = groupState.getOption.getOrElse(initialState)

    val alreadyStoredEvents: Int = oldState.number_of_events
    val newEventsWithIndex = values.zip(Iterator.range(alreadyStoredEvents, alreadyStoredEvents + values.size).toSeq)
    val generatedPairs = mutable.ArrayBuffer[Structs.StreamingPair]()

    for (evb <- newEventsWithIndex) {
      //First handle the pair of the same event type - and define the previous event of the same type.
      //It is null if this is the first time we find this pair
      val same_type_events: Array[(Timestamp, Int)] = oldState.events.getOrElse(evb._1.event_type, Array.empty[(Timestamp, Int)])
      var prev_event: (String, Timestamp, Int) = null
      if (same_type_events.isEmpty) {
        oldState.events(evb._1.event_type) = Array((evb._1.timestamp, evb._2))
      } else {
        if (same_type_events.length == 1) { //there is only one event -> append the new one
          //get the previous event
          prev_event = (evb._1.event_type, same_type_events.head._1, same_type_events.head._2)
          oldState.events(evb._1.event_type) = oldState.events(evb._1.event_type) :+ (evb._1.timestamp, evb._2)
        } else { //there are two events, replace the second one
          prev_event = (evb._1.event_type, same_type_events(1)._1, same_type_events(1)._2)
          oldState.events(evb._1.event_type)(1) = (evb._1.timestamp, evb._2)
        }
        val event = StreamingProcess.createPair(prev_event, evb)
        generatedPairs.append(event)
      }

      // Create the pairs (a,b) for different event types. For each event will go through each other type and starting
      // from the first event of that type will generate a pair if this event is between the prev event and the new event.
      // if the prev event is null it will generate pair if the event is before that timestamp of the new event.

      // that is the first appearance of this event type, create pairs with every first event of the other keys
      val evb_transformed = (evb._1.event_type, evb._1.timestamp, evb._2)
      val keys = oldState.events.keys.toSeq
      if (prev_event == null) {
        for (key <- keys) {
          if (key != evb._1.event_type) { //for all the other keys
            val eva = oldState.events(key)(0)
            val eva_transformed = (key, eva._1, eva._2)
            generatedPairs.append(createPair(eva_transformed, evb_transformed, traceId.toInt))
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
                generatedPairs.append(createPair(eva_transformed, evb_transformed, traceId.toInt))
                i = 3
              } else {
                i += 1;
              }
            }
          }
        }
      }
    }
    //Update the previous state with the new one
    val newState = new CustomState(oldState.number_of_events + values.size, oldState.events)
    groupState.update(newState)
    // return the pairs
    generatedPairs.iterator
  }

  def calculateMetrics(eventPair:(String,String), c:Iterator[Structs.Count],groupState:GroupState[CountState]):Iterator[Structs.Count]={
    val values = c.toSeq
    val initialState = new CountState(0, 0,Long.MaxValue,0)
    val oldState = groupState.getOption.getOrElse(initialState)

    val sum_durations =values.map(_.sum_duration).sum + oldState.sum_duration
    val min_duration = Math.min(values.map(_.min_duration).min,oldState.min_duration)
    val max_duration = Math.max(values.map(_.max_duration).max,oldState.max_duration)
    val counts = values.size+oldState.count

    val newState = CountState(sum_durations,counts,min_duration, max_duration)
    groupState.update(newState)
    Iterator(Structs.Count(eventPair._1,eventPair._2,sum_durations,counts,min_duration, max_duration))
  }




}
