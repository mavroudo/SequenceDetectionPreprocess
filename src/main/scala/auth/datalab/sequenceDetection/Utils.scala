package auth.datalab.sequenceDetection

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import auth.datalab.sequenceDetection.Structs.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deckfour.xes.in.XParserRegistry
import org.deckfour.xes.model.{XLog, XTrace}

import scala.collection.JavaConversions._


object Utils {

  /**
   * Method to read environment variables
   *
   * @param key The key of the variable
   * @return The variable
   */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
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

  def readFromTxt(fileName: String, seperator: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.textFile(fileName).zipWithIndex map { case (line, index) =>
      val sequence = line.split(seperator).zipWithIndex map { case (event, inner_index) =>
        Structs.Event(inner_index.toString, event)
      }
      Structs.Sequence(sequence.toList, index)
    }
  }

  def readFromXes(fileName: String): RDD[Structs.Sequence] = {
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

    val df = new SimpleDateFormat("MMM d, yyyy HH:mm:ss a") //read this pattern from xes
    val df3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val df4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // transform it to this patter


    val data = parsed_logs.head.zipWithIndex map { case (trace: XTrace, index: Int) =>
      val list = trace.map(event => {
        val event_name = event.getAttributes.get("concept:name").toString
        val timestamp_occurred = event.getAttributes.get("time:timestamp").toString
        Event(df2.format(df4.parse(timestamp_occurred)), event_name)
      }).toList
      Structs.Sequence(list, index.toLong)
    }
    val par = spark.sparkContext.parallelize(data)
    par
  }

  def readWithTimestamps(fileName: String, seperator: String, delimiter: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.textFile(fileName).map({ line =>
      val index = line.split("::")(0).toInt
      val events = line.split("::")(1)
      val sequence = events.split(seperator).map(event => {
        Structs.Event(event.split(delimiter)(1), event.split(delimiter)(0))
      })
      Structs.Sequence(sequence.toList, index)
    })
  }

  /**
   * Return if a is less than b
   *
   * @param timeA first time
   * @param timeB second time
   * @return
   */
  def compareTimes(timeA: String, timeB: String): Boolean = {
    if (timeA == "") {
      return true
    }
    try {
      timeA.toInt < timeB.toInt
    } catch {
      case _: Throwable =>
        !Timestamp.valueOf(timeA).after(Timestamp.valueOf(timeB))
    }

  }

  /**
   * Method for sorting entries based on their timestamps
   *
   * @param s1 Timestamp in string format
   * @param s2 Timestamp in string format
   * @return True or false based on order
   */
  def sortByTime(s1: String, s2: String): Boolean = {
    val time1 = Timestamp.valueOf(s1)
    val time2 = Timestamp.valueOf(s2)
    time1.before(time2)
  }

  /**
   * Method to return the difference in milliseconds between timestamps
   *
   * @param pr_time  The 1st timestamp in string format
   * @param new_time The 2nd timestamp in string format
   * @return The difference in long format
   */
  def getDifferenceTime(pr_time: String, new_time: String): Long = {
    val time_pr = Timestamp.valueOf(pr_time)
    val time_new = Timestamp.valueOf(new_time)
    val res = Math.abs(time_new.getTime - time_pr.getTime)
    res
  }

}
