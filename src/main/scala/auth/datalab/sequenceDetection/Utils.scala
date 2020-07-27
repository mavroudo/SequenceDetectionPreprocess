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

  def readFromTxt(fileName: String, seperator: String, hasTimestamp: Boolean): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.textFile(fileName).zipWithIndex map { case (line, index) =>
      val sequence = line.split(seperator).zipWithIndex map { case (event, inner_index) =>
        Structs.Event(inner_index.toString, event)
      }
      Structs.Sequence(sequence.toList, index)
    }
  }

  def readFromXes(fileName: String)= {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val file_Object = new File(fileName)
    var parsed_logs: List[XLog] = null
    val parsers_iterator = XParserRegistry.instance().getAvailable.iterator()
    while (parsers_iterator.hasNext) {
      var p = parsers_iterator.next
      if (p.canParse(file_Object)) {
        parsed_logs = p.parse(new FileInputStream(file_Object)).toList
      }
    }
    val pattern = "MMM d, yyyy HH:mm:ss a"
    val df = new SimpleDateFormat(pattern)
    val df2 = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")

    val data=parsed_logs(0).zipWithIndex map { case (trace:XTrace, index:Int) =>
      val list =trace.map(event=>{
        val event_name = event.getAttributes.get("concept:name").toString
        val timestamp_occurred=event.getAttributes.get("time:timestamp").toString
        Event(df2.format(df.parse(timestamp_occurred)), event_name)
      }).toList
      Structs.Sequence(list,index.toLong)
    }
    val par = spark.sparkContext.parallelize(data)
    par
  }

  /**
   * Return if a is less than b
   * @param timeA
   * @param timeB
   * @return
   */
  def compareTimes(timeA:String,timeB:String):Boolean={
    if (timeA==""){
      return true
    }
    val dateFormat=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss")
    try {
      timeA.toInt < timeB.toInt
    }catch{
      case e : Throwable => {
        e.getMessage()
        dateFormat.parse(timeA).before(dateFormat.parse(timeB))
      }
    }

  }


}
