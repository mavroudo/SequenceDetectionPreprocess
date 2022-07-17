package auth.datalab.sequenceDetection.ObjectStorage


import auth.datalab.sequenceDetection.ObjectStorage.Storage.{CountTable, IndexTable, SequenceTable, SingleTable}
import auth.datalab.sequenceDetection.Structs.Sequence
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Preprocess {
  case class SequenceP(events: List[EventP], trace_id: Long)

  case class EventP(event_type: String, position: Long, timestamp: String)

  var join: Boolean = false
  var overwrite: Boolean = false
  var splitted_dataset: Boolean = false

  def execute(sequenceRDD: RDD[Sequence], log_name: String, overwrite: Boolean, join: Boolean, splitted_dataset: Boolean): Long = {
    this.overwrite = overwrite
    this.join = join
    this.splitted_dataset = splitted_dataset
    val start = System.currentTimeMillis()

    SingleTable.writeTable(sequenceRDD, log_name, overwrite, join, splitted_dataset)
    val seqs = SequenceTable.writeTable(sequenceRDD, log_name, overwrite, join, splitted_dataset)
//    seqs.persist(StorageLevel.MEMORY_AND_DISK)
    val idx = IndexTable.writeTable(seqs, log_name, overwrite, join, splitted_dataset)
//    idx.persist(StorageLevel.MEMORY_AND_DISK)
//    seqs.unpersist()
    CountTable.writeTable(idx, log_name, overwrite, join, splitted_dataset)
//    idx.unpersist()
    System.currentTimeMillis() - start


  }
}
