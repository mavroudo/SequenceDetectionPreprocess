package auth.datalab.siesta.BusinessLogic.IngestData

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deckfour.xes.in.XParserRegistry
import org.deckfour.xes.model.{XLog, XTrace}

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import scala.collection.JavaConversions.asScalaBuffer

/**
 * This file defines the various formats that can be ingested in the system
 */
object ReadLogFile {
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
        Structs.Event(df2.format(df4.parse(timestamp_occurred)), event_name)
      }).toList
      Structs.Sequence(list, index.toLong)
    }
    val par = spark.sparkContext.parallelize(data)
    par
  }

  def readWithTimestamps(fileName: String, seperator: String, delimiter: String): RDD[Structs.Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    //    spark.sparkContext.textFile(fileName).map({ line =>
    //      val index = line.split("::")(0).toInt
    //      val events = line.split("::")(1)
    //      val sequence = events.split(seperator).map(event => {
    //        Structs.Event(event.split(delimiter)(1), event.split(delimiter)(0))
    //      })
    //      Structs.Sequence(sequence.toList, index)
    //    })

    spark.read.text(fileName).rdd.map(row => row.getString(0))
      .map({ line =>
        val index = line.split("::")(0).toInt
        val events = line.split("::")(1)
        val sequence = events.split(seperator).map(event => {
          Structs.Event(event.split(delimiter)(1), event.split(delimiter)(0))
        })
        Structs.Sequence(sequence.toList, index)
      })
  }

}
