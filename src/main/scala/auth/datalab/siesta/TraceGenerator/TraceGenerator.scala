package auth.datalab.siesta.TraceGenerator

import auth.datalab.siesta.BusinessLogic.Model.{Event, Sequence, Structs}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * This class is responsible to generate random traces of random event types. All the trace will have length
 * randomly picked (using uniform distribution) between minTraceSize and maxTraceSize It can be used to evaluate the
 * performance of SIESTA in throughput and correctness.
 *
 * @param numberOfTraces The number of traces to be produced
 * @param numberOfDifferentActivities The number of different event types
 * @param minTraceSize The minimum trace length
 * @param maxTraceSize The maximum trace length
 */
class TraceGenerator (val numberOfTraces:Int, val numberOfDifferentActivities:Int, val minTraceSize:Int, val maxTraceSize:Int) extends Serializable {
  private def gen: Stream[Char] = Random.alphanumeric

  //a uniform generator will be used for the traces, so total size will be (max-min)/2+min*numberOfTraces
  private val activities: mutable.HashSet[String] = new mutable.HashSet[String]()
  this.findActivities()

  /**
   * Utilizes the createSequence function (private) to generate random traces and then use spark to parallelize distribute
   * them
   *
   * @return An RDD that contains the randomly generated traces
   */
  def produce(): RDD[Sequence] = {
    val traces = List.fill(numberOfTraces)(minTraceSize + Random.nextInt((maxTraceSize - minTraceSize) + 1))
    val spark = SparkSession.builder().getOrCreate()
    val parallelized = spark.sparkContext.parallelize(traces.zipWithIndex)
    val bac = spark.sparkContext.broadcast(activities.toList)
    parallelized.map(x => {
      createSequence(bac, x._1, x._2)
    })
  }


  /**
   * Utilizes the createSequence function (private) to generate random traces and then use spark to parallelize distribute
   * them. The difference with the produce() function, is that this one generate traces with ids that are passed as
   * parameters
   *
   * @param t A list with the trace ids
   * @return An RDD that contains the randomly generated traces
   */
  def produce(t: Iterable[Long]): RDD[Sequence] = {
    val traces = List.fill(t.size)(minTraceSize + Random.nextInt((maxTraceSize - minTraceSize) + 1))
    val spark = SparkSession.builder().getOrCreate()
    val parallelized = spark.sparkContext.parallelize(traces.zip(t))
    val bac = spark.sparkContext.broadcast(activities.toList)
    parallelized.map(x => {
      createSequence(bac, x._1, x._2)
    })
  }

  /**
   * Utilizes the createSequence function (private) to generate random traces and then use spark to parallelize distribute
   * them. The difference with the produce() function, is that this one generate traces with ids that are passed as
   * parameters
   *
   * @param t A list with the trace ids
   * @return An RDD that contains the randomly generated traces
   */
  def produce(t: List[Int]): RDD[Sequence] = {
    produce(t.map(_.toLong))
  }


  /**
   * Based on the Broadcasted List with the event types it generates a random trace. The generated timestamps start
   * from a hardcoded timestamp "1600086941" and the difference between two consecutive timestamps can be up to
   * 2 hours (picked randomly using uniform distribution)
   *
   * @param ac The broadcasted event types
   * @param events The trace length
   * @param id The id of the generated trace
   * @return The generated trace
   */
  private def createSequence(ac:Broadcast[List[String]], events:Int, id:Long):Sequence={
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //final format
    var starting_date = 1600086941
    val r = scala.util.Random
    val listevents:ListBuffer[Event] = new ListBuffer[Event]()
    for(_<- 0 until events){
      val activity = ac.value.lift(Random.nextInt(ac.value.size))
      //get random timestamp
      starting_date+=r.nextInt(60*60*2) //up to 2 hours
      val timestamp:String= df2.format(new Date(starting_date))
      listevents.append(new Event(timestamp,activity.get))
    }
    new Sequence(listevents.toList,id)
  }

  /**
   * Generates N random event types and stores them in the [[activities]] variable, where n is the
   * [[numberOfDifferentActivities]]
   */
  private def findActivities(): Unit ={
    val length:Int = math.round(math.sqrt(numberOfDifferentActivities)).toInt
    while(activities.size<numberOfDifferentActivities){
      val activity: String = get(length)
      if(!activities.contains(activity)){
        activities+=activity
      }
    }
  }

  /**
   * Generates a single event type, utilizing the [[gen]]
   * @param len The number of letters in the event type name
   * @return An event type
   */
  private def get(len: Int): String = {
    @tailrec
    def build(acc: String, s: Stream[Char]): String = {
      if (s.isEmpty) acc
      else build(acc + s.head, s.tail)
    }
    build("", gen take len)
  }
}
