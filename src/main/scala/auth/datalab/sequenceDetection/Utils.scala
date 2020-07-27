package auth.datalab.sequenceDetection

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import auth.datalab.sequenceDetection.Structs.Event
import javax.swing.JPopupMenu.Separator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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

  def readLog(fileName:String,separator: String=","):RDD[Structs.Sequence]={
    if (fileName.split('.')(1) == "txt") { //there is no time limitations
      this.readFromTxt(fileName, separator).persist(StorageLevel.MEMORY_AND_DISK)
    } else if (fileName.split('.')(1) == "xes") {
      this.readFromXes(fileName).persist(StorageLevel.MEMORY_AND_DISK)
    }else{
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

  def readFromXes(fileName: String):RDD[Structs.Sequence]= {
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

    val df = new SimpleDateFormat("MMM d, yyyy HH:mm:ss a" ) //read this pattern from xes
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // transform it to this patter

    val data=parsed_logs.head.zipWithIndex map { case (trace:XTrace, index:Int) =>
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
    try {
      timeA.toInt < timeB.toInt
    }catch{
      case e : Throwable =>
        Timestamp.valueOf(timeA).before(Timestamp.valueOf(timeB))
    }

  }


}
