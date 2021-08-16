package auth.datalab.sequenceDetection.SetContainment
import auth.datalab.sequenceDetection.PairExtraction.{Indexing, Parsing, SkipTillAnyMatch, State, StrictContiguity}
import auth.datalab.sequenceDetection.SequenceDetection.cassandraConnection
import auth.datalab.sequenceDetection.SetContainment.CassandraSetContainment
import auth.datalab.sequenceDetection.{CassandraConnection, Structs, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object SetContainment {
  case class SetCInverted(event1: String, event2: String, ids: List[Long])
  private var cassandraConnection: CassandraSetContainment = null

  def main(args: Array[String]): Unit = {
    val fileName: String = args(0)
    val type_of_algorithm = args(1) //parsing, indexing or state
    val deleteAll = args(2)
    val join = args(3).toInt
    val deletePrevious = args(4)
    println(fileName, type_of_algorithm, deleteAll, join)
    var logName = fileName.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    Logger.getLogger("org").setLevel(Level.ERROR)

    cassandraConnection = new CassandraSetContainment()
    cassandraConnection.startSpark()
    if (deletePrevious == "1" || deleteAll == "1") {
      cassandraConnection.dropTable(logName)
    }
    cassandraConnection.createTable(logName)
    val sequencesRDD: RDD[Structs.Sequence] = Utils.readLog(fileName)
    sequencesRDD.persist(StorageLevel.MEMORY_AND_DISK)
    val inverted_index = createCombinationsRDD(sequencesRDD,type_of_algorithm)
    cassandraConnection.writeTableSequenceIndex(inverted_index,logName)
    cassandraConnection.writeTableSeq(sequencesRDD,logName)
    inverted_index.unpersist()
    sequencesRDD.unpersist()
    cassandraConnection.closeSpark()
  }


  def createCombinationsRDD(seqRDD: RDD[Structs.Sequence], type_of_algorithm: String): RDD[SetCInverted] = {
    val combinations = type_of_algorithm match {
      case "parsing" => Parsing.extract(seqRDD)
      case "indexing" => Indexing.extract(seqRDD)
      case "state" => State.extract(seqRDD)
      case "strict" =>StrictContiguity.extract(seqRDD)
      case "anymatch" => SkipTillAnyMatch.extract(seqRDD)
      case _ => throw new Exception("Wrong type of algorithm")
    }
    combinations.map(pair=>{
      val l= pair.times.map(_.id).distinct.sortWith((x,y)=>x<y)
      SetCInverted(pair.event1,pair.event2,l)
    })
      .persist(StorageLevel.MEMORY_AND_DISK)

  }


}