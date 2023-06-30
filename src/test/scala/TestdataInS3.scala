import auth.datalab.siesta.BusinessLogic.Metadata.MetaData
import auth.datalab.siesta.BusinessLogic.Model.Structs.InvertedSingleFull
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.S3Connector.S3Connector
import org.apache.spark.rdd.RDD


object TestdataInS3 {

  def main(args: Array[String]):Unit={
    println("Hello")
    val c:Config = Config()
    val dbConnector = new S3Connector()
    dbConnector.initialize_spark(c)
    dbConnector.initialize_db(config = c)

    val metadata:MetaData = dbConnector.get_metadata(c)
    val inv:RDD[InvertedSingleFull] = dbConnector.read_single_table(metadata)
    val records = inv.count()
    val seq = dbConnector.read_sequence_table(metadata)
    val traces = seq.count()
    val events = seq.map(x=>x.events.size).sum()

    val count = dbConnector.read_count_table(metadata)

    val last = dbConnector.read_last_checked_table(metadata)

    val index = dbConnector.read_index_table(metadata)

    println("Worked")
  }

}
