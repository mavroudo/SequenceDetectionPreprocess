package auth.datalab.sequenceDetection
import auth.datalab.sequenceDetection.Signatures.Signature
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf()
      .setAppName("FA Indexing")
      .setMaster("local[*]")


    val spark = SparkSession.builder().config(configuration).getOrCreate()

    val tg:TraceGenerator = new TraceGenerator(10000,1000,5000,15000)
    println(tg.produce().count())

//    if(args(5)=="normal"){
//      SequenceDetection.main(args)
//    }else if(args(5)=="signature"){
//      Signature.main(args)
//    }else if(args(5)=="setcontainment"){
//      SetContainment.SetContainment.main(args)
//    }else{
//      println("not a valid choice")
//    }
  }

}
