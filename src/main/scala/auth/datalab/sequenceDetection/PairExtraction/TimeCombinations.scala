package auth.datalab.sequenceDetection.PairExtraction

import auth.datalab.sequenceDetection.Structs
import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object TimeCombinations {

  /***
   * Cleanes the RDD from pairs that are completed before @time
   * @param data
   * @param time
   * @return
   */
  def timeCombinationsRDD(data: RDD[Structs.EventIdTimeLists], time: String): RDD[Structs.EventIdTimeLists] = {
    if (time == ""){
      return data
    }
    val spark = SparkSession.builder().getOrCreate()

    val res = data
      .map(l => {
        val newList = l.times.map(r => {
          val newLine = extractTimesPairs(r, time)
          newLine
        }).filter(_.times.nonEmpty)
        Structs.EventIdTimeLists(l.event1, l.event2, newList)
      }).filter(_.times.nonEmpty)
      .coalesce(spark.sparkContext.defaultParallelism)
    res
  }

  /***
   * Remove all the pairs form the line that their completion time is before time
   * @param line
   * @param time We accept all pairs that ends after this time
   * @return
   */
  private def extractTimesPairs(line: Structs.IdTimeList, time: String): Structs.IdTimeList = {

    var toDrop = 0
    val timeTimestamp = Timestamp.valueOf(time) //the starting timestamp
    breakable {
      for (i <- 1 until line.times.length by 2) {
        val newTime = Timestamp.valueOf(line.times(i))
        if (newTime.before(timeTimestamp)) {
          toDrop += 1
        } else {
          break()
        }
      }
    }
    val newLine = line.times.drop(toDrop * 2)
    Structs.IdTimeList(line.id, newLine)
  }

}
