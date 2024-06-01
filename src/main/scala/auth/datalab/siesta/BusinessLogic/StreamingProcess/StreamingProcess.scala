package auth.datalab.siesta.BusinessLogic.StreamingProcess

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.BusinessLogic.Model.Structs.EventStream
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3ConnectorStreaming.DatabaseConnector
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp
import scala.collection.mutable

object StreamingProcess {

  case class CustomState(number_of_events: Int, events: mutable.HashMap[String, Array[(Timestamp, Int)]]) extends Serializable // state 1

  case class CountState(sum_duration:Long,count:Int,min_duration:Long,max_duration:Long) extends Serializable // state 3

  case class LastEventState(lastEvent:mutable.HashMap[Long,Timestamp]) // state 3

  implicit val stateEncoder: Encoder[CustomState] = Encoders.kryo[CustomState]

  implicit  val countStateEncoder: Encoder[CountState] = Encoders.kryo[CountState]

  var state_storage = ""

  def initialize(config: Config): Unit = {
    state_storage = config.state_storage
    if (config.state_storage.equals("postgres")) {
      val connection = DatabaseConnector.getConnection
      try {
        DatabaseConnector.createCustomStateTableIfNotExists(connection)
        DatabaseConnector.createCountStateTableIfNotExists(connection)
      } finally {
        if (connection != null) connection.close()
      }
    }
  }

  def createPair(eva:(String,Timestamp,Int),evb:(EventStream,Int)):Structs.StreamingPair={
    Structs.StreamingPair(eva._1, evb._1.event_type, evb._1.trace,eva._2, evb._1.timestamp, eva._3, evb._2)
  }

  def createPair(eva:(String,Timestamp,Int),evb:(String,Timestamp,Int),traceId:Int):Structs.StreamingPair={
    Structs.StreamingPair(eva._1, evb._1, traceId,eva._2, evb._2, eva._3, evb._3)
  }

  def insertCustomStateToPostgres(traceId: Long, state: CustomState): Unit = {
    val connection = DatabaseConnector.getConnection
    connection.setAutoCommit(false)

    val insertStateQuery = "INSERT INTO custom_state (id, number_of_events) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET number_of_events = EXCLUDED.number_of_events"
    val statePreparedStatement = connection.prepareStatement(insertStateQuery)
    statePreparedStatement.setLong(1, traceId)
    statePreparedStatement.setInt(2, state.number_of_events)
    statePreparedStatement.executeUpdate()

    val insertEventQuery = "INSERT INTO custom_state_events (state_id, event_key, event_timestamp, event_value) VALUES (?, ?, ?, ?)"
    val eventPreparedStatement = connection.prepareStatement(insertEventQuery)

    state.events.foreach { case (key, eventArray) =>
      eventArray.foreach { case (timestamp, value) =>
        eventPreparedStatement.setLong(1, traceId)
        eventPreparedStatement.setString(2, key)
        eventPreparedStatement.setTimestamp(3, timestamp)
        eventPreparedStatement.setInt(4, value)
        eventPreparedStatement.addBatch()
      }
    }

    eventPreparedStatement.executeBatch()
    connection.commit()

    statePreparedStatement.close()
    eventPreparedStatement.close()
    connection.close()
  }

  def retrieveCustomStateFromPostgres(traceId: Long): CustomState = {
    val connection = DatabaseConnector.getConnection

    val selectStateQuery = "SELECT number_of_events FROM custom_state WHERE id = ?"
    val statePreparedStatement = connection.prepareStatement(selectStateQuery)
    statePreparedStatement.setLong(1, traceId)
    val stateResultSet = statePreparedStatement.executeQuery()

    var numberOfEvents = 0
    if (stateResultSet.next()) {
      numberOfEvents = stateResultSet.getInt("number_of_events")
    }

    val selectEventQuery = "SELECT event_key, event_timestamp, event_value FROM custom_state_events WHERE state_id = ?"
    val eventPreparedStatement = connection.prepareStatement(selectEventQuery)
    eventPreparedStatement.setLong(1, traceId)
    val eventResultSet = eventPreparedStatement.executeQuery()

    val events = mutable.HashMap[String, Array[(Timestamp, Int)]]()
    while (eventResultSet.next()) {
      val key = eventResultSet.getString("event_key")
      val timestamp = eventResultSet.getTimestamp("event_timestamp")
      val value = eventResultSet.getInt("event_value")
      val eventArray = events.getOrElse(key, Array())
      events.update(key, eventArray :+ (timestamp -> value))
    }

    stateResultSet.close()
    statePreparedStatement.close()
    eventResultSet.close()
    eventPreparedStatement.close()
    connection.close()

    CustomState(numberOfEvents, events)
  }

  def upsertCountState(eventPair: String, state: CountState): Unit = {
    val connection = DatabaseConnector.getConnection
    connection.setAutoCommit(false)

    val upsertStateQuery =
      """
        |INSERT INTO count_state (event_pair, sum_duration, count, min_duration, max_duration)
        |VALUES (?, ?, ?, ?, ?)
        |ON CONFLICT (event_pair) DO UPDATE
        |SET sum_duration = EXCLUDED.sum_duration,
        |    count = EXCLUDED.count,
        |    min_duration = EXCLUDED.min_duration,
        |    max_duration = EXCLUDED.max_duration
      """.stripMargin
    val preparedStatement = connection.prepareStatement(upsertStateQuery)
    preparedStatement.setString(1, eventPair)
    preparedStatement.setLong(2, state.sum_duration)
    preparedStatement.setInt(3, state.count)
    preparedStatement.setLong(4, state.min_duration)
    preparedStatement.setLong(5, state.max_duration)
    preparedStatement.executeUpdate()

    preparedStatement.close()
    connection.commit()
    connection.close()
  }

  def retrieveCountState(eventPair: String): CountState = {
    val connection = DatabaseConnector.getConnection

    val selectStateQuery = "SELECT sum_duration, count, min_duration, max_duration FROM count_state WHERE event_pair = ?"
    val preparedStatement = connection.prepareStatement(selectStateQuery)
    preparedStatement.setString(1, eventPair)
    val resultSet = preparedStatement.executeQuery()

    val initialState = new CountState(0, 0, Long.MaxValue, Long.MinValue)
    val state = if (resultSet.next()) {
      CountState(
        resultSet.getLong("sum_duration"),
        resultSet.getInt("count"),
        resultSet.getLong("min_duration"),
        resultSet.getLong("max_duration")
      )
    } else {
      initialState
    }

    resultSet.close()
    preparedStatement.close()
    connection.close()

    state
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

    var oldState: CustomState = null
    if (state_storage.equals("default")) {
      val initialState = new CustomState(0, new mutable.HashMap[String, Array[(Timestamp, Int)]])
      oldState = groupState.getOption.getOrElse(initialState)
    } else if (state_storage.equals("rocksDB")) {
      oldState = RocksDBClient.getState(traceId.toString + "_trace")
        .map(bytes => SerializationUtils.deserialize(bytes).asInstanceOf[CustomState])
        .getOrElse(new CustomState(0, mutable.HashMap[String, Array[(Timestamp, Int)]]()))
    } else if (state_storage.equals("postgres")) {
      oldState = retrieveCustomStateFromPostgres(traceId)
    }

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
                i += 1
              }
            }
          }
        }
      }
    }

    val newState = new CustomState(oldState.number_of_events + values.size, oldState.events)
    if (state_storage.equals("default")) {
      groupState.update(newState)
    } else if (state_storage.equals("rocksDB")) {
      RocksDBClient.saveState(traceId.toString + "_trace", SerializationUtils.serialize(newState))
    } else if (state_storage.equals("postgres")) {
      insertCustomStateToPostgres(traceId, newState)
    }

    generatedPairs.iterator
  }


  def calculateMetrics(eventPair:(String,String), c:Iterator[Structs.Count],groupState:GroupState[CountState]):Iterator[Structs.Count]={
    val values = c.toSeq
    var oldState: CountState = null
    val eventPairStr = eventPair._1 + eventPair._2

    if (state_storage.equals("default")) {
      val initialState = new CountState(0, 0, Long.MaxValue, 0)
      oldState = groupState.getOption.getOrElse(initialState)
    } else if (state_storage.equals("rocksDB")) {
      oldState = RocksDBClient.getState(eventPairStr)
        .map(bytes => SerializationUtils.deserialize(bytes).asInstanceOf[CountState])
        .getOrElse(new CountState(0, 0, Long.MaxValue, Long.MinValue))
    } else if (state_storage.equals("postgres")) {
      oldState = retrieveCountState(eventPairStr)
    }

    val sum_durations =values.map(_.sum_duration).sum + oldState.sum_duration
    val min_duration = Math.min(values.map(_.min_duration).min,oldState.min_duration)
    val max_duration = Math.max(values.map(_.max_duration).max,oldState.max_duration)
    val counts = values.size+oldState.count

    val newState = CountState(sum_durations,counts,min_duration, max_duration)

    if (state_storage.equals("default")) {
      groupState.update(newState)
    } else if (state_storage.equals("rocksDB")) {
      RocksDBClient.saveState(eventPairStr, SerializationUtils.serialize(newState))
    } else if (state_storage.equals("postgres")) {
      upsertCountState(eventPairStr, newState)
    }

    Iterator(Structs.Count(eventPair._1,eventPair._2,sum_durations,counts,min_duration, max_duration))
  }


  /**
   * Maintains the last event of each trace in order to remove late arriving events
   * @param traceId The id of the trace
   * @param eventStream The events in the last batch
   * @param groupState The state that maintains the last event per trace
   * @return The events that occur after the last stored event in this trace
   */
  def filterEvents(traceId:Long, eventStream:Iterator[Structs.EventStream],groupState: GroupState[LastEventState]):Iterator[Structs.EventStream]={
    val values = eventStream.toSeq
    val initialState = new LastEventState(new mutable.HashMap[Long,Timestamp]())
    val oldState = groupState.getOption.getOrElse(initialState)

    val lastEvent = oldState.lastEvent.getOrElse(traceId,null)
    if (lastEvent==null){ //there is no previous event in this trace
      oldState.lastEvent(traceId)=values.last.timestamp
      groupState.update(oldState)
      eventStream
    }else{ // there are already data in this trace
      val filtered = values.filter(x=>x.timestamp.after(lastEvent))
      if(filtered.nonEmpty){
        oldState.lastEvent(traceId)=filtered.last.timestamp
        groupState.update(oldState)
      }
      filtered.iterator
    }

  }




}
