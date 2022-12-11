package auth.datalab.siesta.TraceGenerator

import auth.datalab.siesta.BusinessLogic.Model.Structs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class TraceGenerator (val numberOfTraces:Int, val numberOfDifferentActivities:Int, val minTraceSize:Int, val maxTraceSize:Int) extends Serializable {
  private def gen: Stream[Char] = Random.alphanumeric

  //a uniform generator will be used for the traces, so total size will be (max-min)/2+min*numberOfTraces
  private val activities: mutable.HashSet[String] = new mutable.HashSet[String]()
  this.findActivities()

  def produce(): RDD[Structs.Sequence] = {
    val traces = List.fill(numberOfTraces)(minTraceSize + Random.nextInt((maxTraceSize - minTraceSize) + 1))
    val spark = SparkSession.builder().getOrCreate()
    val parallelized = spark.sparkContext.parallelize(traces.zipWithIndex)
    val bac = spark.sparkContext.broadcast(activities.toList)
    parallelized.map(x => {
      createSequence(bac, x._1, x._2)
    })
  }

  def getActivities: mutable.Set[String] = {
    this.activities
  }

  def produce(t: List[Int]): RDD[Structs.Sequence] = {
    val traces = List.fill(t.size)(minTraceSize + Random.nextInt((maxTraceSize - minTraceSize) + 1))
    val spark = SparkSession.builder().getOrCreate()
    val parallelized = spark.sparkContext.parallelize(traces.zip(t))
    val bac = spark.sparkContext.broadcast(activities.toList)
    parallelized.map(x => {
      createSequence(bac, x._1, x._2)
    })
  }

  def produce(t: Iterable[Long]): RDD[Structs.Sequence] = {
    val traces = List.fill(t.size)(minTraceSize + Random.nextInt((maxTraceSize - minTraceSize) + 1))
    val spark = SparkSession.builder().getOrCreate()
    val parallelized = spark.sparkContext.parallelize(traces.zip(t))
    val bac = spark.sparkContext.broadcast(activities.toList)
    parallelized.map(x => {
      createSequence(bac, x._1, x._2)
    })

  }
  def estimate_size(): Structs.Sequence = {
    val spark = SparkSession.builder().getOrCreate()
    val bac = spark.sparkContext.broadcast(activities.toList)
    createSequence(bac, (minTraceSize + maxTraceSize) / 2, 1)

  }


  private def createSequence(ac:Broadcast[List[String]], events:Int, id:Long):Structs.Sequence={
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //final format
    var starting_date = 1600086941
    val r = scala.util.Random
    val listevents:ListBuffer[Structs.Event] = new ListBuffer[Structs.Event]()
    for(_<- 0 until events){
      val activity = ac.value.lift(Random.nextInt(ac.value.size))
      //get random timestamp
      starting_date+=r.nextInt(60*60*2) //up to 2 hours
      val timestamp:String= df2.format(new Date(starting_date))
      listevents.append(Structs.Event(timestamp,activity.get))
    }
    Structs.Sequence(listevents.toList,id)
  }

  private def findActivities(): Unit ={
    val length:Int = math.round(math.sqrt(numberOfDifferentActivities)).toInt
    while(activities.size<numberOfDifferentActivities){
      val activity: String = get(length)
      if(!activities.contains(activity)){
        activities+=activity
      }
    }
  }

  private def get(len: Int): String = {
    @tailrec
    def build(acc: String, s: Stream[Char]): String = {
      if (s.isEmpty) acc
      else build(acc + s.head, s.tail)
    }

    build("", gen take len)
  }
}
