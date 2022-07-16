package auth.datalab.sequenceDetection.IndexWithPositions

import auth.datalab.sequenceDetection.CommandLineParser.Utilities.Iterations
import auth.datalab.sequenceDetection.CommandLineParser.{Config, Utilities}
import auth.datalab.sequenceDetection.{CountPairs, Structs}
import auth.datalab.sequenceDetection.PairExtraction.Indexing
import auth.datalab.sequenceDetection.SIESTA.Preprocess
import auth.datalab.sequenceDetection.Structs.EventIdTimeLists
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object NoTimestamps {
  private var cassandraConnection: CassandraConnection = _

  def execute(c: Config): Unit = {
    val table_name = c.filename.split('/').last.toLowerCase().split('.')(0).split('$')(0).replace(' ', '_')
    cassandraConnection = new CassandraConnection(table_name)
    cassandraConnection.startSpark()
    if (c.delete_previous) {
      cassandraConnection.dropTables()
    }
    if (c.delete_all) {
      cassandraConnection.dropAlltables()
    }
    cassandraConnection.createTables()
    try {
      val init = Utilities.getRDD(c, 100)
      val sequencesRDD_before_repartitioned = init.data
      val traces: Int = Utilities.getTraces(c, sequencesRDD_before_repartitioned)
      val iterations: Iterations = Utilities.getIterations(c, sequencesRDD_before_repartitioned, traces)
      val ids: List[Array[Long]] = Utilities.getIds(c, sequencesRDD_before_repartitioned, traces, iterations.iterations)
      var k = 0L
      var r = 1
      for(id <- ids){
        println(s"""executing $r/${ids.size}""")
        val sequencesRDD: RDD[Structs.Sequence]=Utilities.getNextData(c,sequencesRDD_before_repartitioned,id,init.traceGenerator,iterations.allExecutors)
        cassandraConnection.writeTableSeq(sequencesRDD,cassandraConnection.table_seq)

        Preprocess.createSingle(sequencesRDD,cassandraConnection.table_single,cassandraConnection)
        val indexing = IndexingNoTimestamps.extract(sequencesRDD)
        indexing.persist(StorageLevel.MEMORY_AND_DISK)
        val combinationsCountRDD = CountPairs.createCountCombinationsRDD(indexing.map(x=>{
          EventIdTimeLists(x.event1,x.event2,x.times)
        }))
        cassandraConnection.writeTableSeqCount(combinationsCountRDD,cassandraConnection.table_count)
        cassandraConnection.writeTableIndex(indexing,cassandraConnection.table_idx)
        indexing.unpersist()
        combinationsCountRDD.unpersist()
        sequencesRDD.unpersist()
        r+=1
      }


    } catch {
      case e: Exception =>
        e.getStackTrace.foreach(println)
        println(e.getMessage)
        cassandraConnection.closeSpark()
    }
  }





}
