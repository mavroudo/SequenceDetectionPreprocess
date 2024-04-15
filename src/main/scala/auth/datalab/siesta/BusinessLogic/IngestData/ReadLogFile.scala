package auth.datalab.siesta.BusinessLogic.IngestData

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deckfour.xes.in.XParserRegistry
import org.deckfour.xes.model.{XLog, XTrace}



import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Scanner
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer

/**
 * This class defines the various formats that can be ingested in the system
 */
object ReadLogFile {
  /**
   * This class combines all the different parsing methods and chooses the one that corresponds to the extension of the
   * logfile
   * @param fileName The name of the log file
   * @param separator Defines how the events are separated (can be changed to match new txt format)
   * @return The RDD that contains the parsed traces
   */

  def readLogDetailed(fileName: String, separator: String = ",") : RDD[Structs.DetailedSequence] = {
    if (fileName.split('.')(1) == "xes") {
      this.readFromXesDetailed(fileName)
    } else if (fileName.split('.')(1) == "withTimestamp") {
      this.readWithTimestampsDetailed(fileName, ",", "/delab/")
    } else {
      throw new Exception("Not recognised file type")
    }
  }

  def readLog(fileName: String, separator: String = ","): RDD[Structs.Sequence] = {
    if (fileName.split('.')(1) == "txt") { //there is no time limitations
      this.readFromTxt(fileName, separator)
    } else if (fileName.split('.')(1) == "xes") {
      this.readFromXes(fileName)
    } else if (fileName.split('.')(1) == "withTimestamp") {
      this.readWithTimestamps(fileName, ",", "/delab/")
    } else {
      throw new Exception("Not recognised file type")
    }
  }

  /**
   * This is the simplest file extension. Each line contains information for a particular event. The events are separated
   * by the separator. The trace index is the index of the line. Instead of using the timestamp for each event the index
   * of the event in the trace is utilized.
   * @param fileName The name of the log file
   * @param seperator The separator of the events that belongs in the same trace
   * @return The RDD that contains the parsed traces
   */
  private def readFromTxt(fileName: String, seperator: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.textFile(fileName).zipWithIndex map { case (line, index) =>
      val sequence = line.split(seperator).zipWithIndex map { case (event, inner_index) =>
        Structs.Event(inner_index.toString, event)
      }
      Structs.Sequence(sequence.toList, index)
    }
  }

  private def readWithTimestampsDetailed(fileName: String, separator: String, delimiter: String): RDD[Structs.DetailedSequence] = {
    val spark = SparkSession.builder().getOrCreate()

    val reader = new Scanner(new File(fileName))
    val ar:ArrayBuffer[Structs.DetailedSequence] = new ArrayBuffer[Structs.DetailedSequence]()
    while(reader.hasNextLine){
      val line = reader.nextLine()
      val index = line.split("::")(0).toInt
      val events = line.split("::")(1)
      val sequence = events.split(separator).map(event => {
        new Structs.DetailedEvent(event_type = new Structs.EventType(event.split(delimiter)(0)),
                                  end_timestamp = event.split(delimiter)(1),
                                  resource = event.split(delimiter)(2),
                                  trace_id = index.toString)
      })
      ar.append(new Structs.DetailedSequence(sequence.toList, index))
    }
    val par = spark.sparkContext.parallelize(ar)
    par
  }


  /**
   * Xes files are standard for the Business Process Management. They use an XML format with predefined field names.
   * In order to parse such a file, the [[org.deckfour.xes.in.XParserRegistry]] is utilized.
   * @param fileName The name of the log file
   * @return The RDD that contains the parsed traces
   */
  private def readFromXes(fileName: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    val file_Object = new File(fileName)
    var parsed_logs: List[XLog] = null
    val parsers_iterator = XParserRegistry.instance().getAvailable.iterator()
    while (parsers_iterator.hasNext) {
      val p = parsers_iterator.next
      if (p.canParse(file_Object)) {
        parsed_logs = p.parse(new FileInputStream(file_Object)).toList
      }
    }

    //val df = new SimpleDateFormat("MMM d, yyyy HH:mm:ss a") //read this pattern from xes
    //val df3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val df4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // transform it to this patter


    val data = parsed_logs.head.zipWithIndex map { case (trace: XTrace, index: Int) =>
      val list = trace.map(event => {
        val event_name = event.getAttributes.get("concept:name").toString
        val timestamp_occurred = event.getAttributes.get("time:timestamp").toString
        Structs.Event(df2.format(df4.parse(timestamp_occurred)), event_name)
      }).toList
      Structs.Sequence(list, index.toLong)
    }
    val par = spark.sparkContext.parallelize(data)
    par
  }

  private def readFromXesDetailed(fileName: String): RDD[Structs.DetailedSequence] = {
    val spark = SparkSession.builder().getOrCreate()
    val file_Object = new File(fileName)
    var parsed_logs: List[XLog] = null
    val parsers_iterator = XParserRegistry.instance().getAvailable.iterator()
    while (parsers_iterator.hasNext) {
      val p = parsers_iterator.next
      if (p.canParse(file_Object)) {
        parsed_logs = p.parse(new FileInputStream(file_Object)).toList
      }
    }

    //val df = new SimpleDateFormat("MMM d, yyyy HH:mm:ss a") //read this pattern from xes
    //val df3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val df4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // transform it to this patter

    val data = parsed_logs.head.zipWithIndex map { case (trace: XTrace, index: Int) =>
      val list = trace.map(event => {
        val event_type = new Structs.EventType(event.getAttributes.get("concept:name").toString)
        val end_timestamp = event.getAttributes.get("time:timestamp").toString
        val resource = event.getAttributes.get("org:resource").toString
        val trace_id = trace.getAttributes.get("case:Rfp_id").toString
        new Structs.DetailedEvent(event_type = event_type, end_timestamp = df2.format(df4.parse(end_timestamp)), resource = resource, trace_id = trace_id)
      }).toList
      new Structs.DetailedSequence(list, index.toLong)
    }
    val par = spark.sparkContext.parallelize(data)
    par
  }


  /**
   * WithTimestamps is a custom file format that was used to evaluate the performance of SIESTA as it can be easily
   * transformed to csv files that can be ingested in ELK stack
   * @param fileName The name of the log file
   * @param seperator The separator of the events for a specific trace
   * @param delimiter The separator between the event_type and the event timestamp
   * @return The RDD that contains the parsed traces
   */
  private def readWithTimestamps(fileName: String, seperator: String, delimiter: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()

    val reader = new Scanner(new File(fileName))
    val ar:ArrayBuffer[Structs.Sequence] = new ArrayBuffer[Structs.Sequence]()
    while(reader.hasNextLine){
      val line = reader.nextLine()
      val index = line.split("::")(0).toInt
      val events = line.split("::")(1)
      val sequence = events.split(seperator).map(event => {
        Structs.Event(event.split(delimiter)(1), event.split(delimiter)(0))
      })
      ar.append(Structs.Sequence(sequence.toList, index))
    }
    val par = spark.sparkContext.parallelize(ar)
    par
  }

}
