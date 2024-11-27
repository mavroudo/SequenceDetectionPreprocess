package ConnectionToDBAndIncrement

import auth.datalab.siesta.BusinessLogic.Model.{Event, Sequence, Structs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CreateRDD {

  def createRDD: RDD[Sequence] = {
    val spark = SparkSession.builder().getOrCreate()
    val events: List[Event] = List(new Event("2020-08-15 12:56:42", "a"),
      new Event("2020-08-15 13:49:53", "b"), new Event("2020-08-15 14:21:02", "a"),
      new Event("2020-08-15 14:27:30", "c"), new Event("2020-08-20 15:27:23", "b"))
    val events2: List[Event] = List(new Event("2020-08-15 12:11:54", "a"),
      new Event("2020-08-15 12:39:50", "b"), new Event("2020-08-15 12:45:22", "d"))
    val events3: List[Event] = List(new Event("2020-08-15 12:31:04", "a"),
      new Event("2020-08-15 13:12:59", "b"), new Event("2020-08-15 14:08:49", "a"))
    spark.sparkContext.parallelize(List(new Sequence(events, "0"),new Sequence(events2, "1"), new Sequence(events3, "2")))
  }

  def createRDD_1: List[Sequence] = {
    val events: List[Event] = List(new Event("2020-08-15 12:56:42", "a"),
      new Event("2020-08-16 12:56:42", "b"), new Event("2020-08-19 12:56:42", "a"),
      new Event("2020-08-20 14:21:02", "b"))
    val events2: List[Event] = List(new Event("2020-08-15 12:11:54", "a"),
      new Event("2020-08-16 12:11:54", "c"))
    val events3: List[Event] = List(new Event("2020-08-15 12:31:04", "c"),
      new Event("2020-08-16 12:31:04", "b"), new Event("2020-08-18 12:31:04", "a"))
    List(new Sequence(events, "0"), new Sequence(events2, "1"), new Sequence(events3, "2"))
  }

  def createRDD_2: List[Sequence] = {
    val events: List[Event] = List(new Event("2020-09-03 12:56:42", "a"),
      new Event("2020-09-05 12:56:42", "b"))
    val events2: List[Event] = List(new Event("2020-08-19 12:11:54", "a"))
    val events3: List[Event] = List(new Event("2020-09-07 12:31:04", "c"),
      new Event("2020-09-08 12:31:04", "a"))
    List(new Sequence(events, "0"), new Sequence(events2, "1"), new Sequence(events3, "2"))
  }

}
