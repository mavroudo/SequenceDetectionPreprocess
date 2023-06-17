package auth.datalab.siesta.CassandraConnector

import scala.collection.mutable

/**
 * This class contains the structure and the names of the tables in Cassandra for a given log database name.
 * Additionally it contains the match between the compression algorithms set to the parameters and the compression
 * classes tht needs to be set in Cassandra in order to facilitate compression
 */
object CassandraTables {

  /**
   * Returns the structure of the 6 tables that will be created in Cassandra. For each table (except Metadata) a
   * case class has been created in [[ApacheCassandraTransformations]]. That is, if data are transformed into these
   * objects they can be directly stored in Cassandra using the spark api.
   * @param logname The log database name
   * @return The map of table names and the command to create them in Cassandra.
   */
  def getTablesStructures(logname:String):Map[String,String]={
    val tableMap:mutable.HashMap[String,String] = new mutable.HashMap[String,String]()
    tableMap+=((logname+"_meta","key text, value text, PRIMARY KEY (key)"))
    tableMap+=((logname+"_seq","sequence_id text, events list<text>, PRIMARY KEY (sequence_id)"))
    tableMap+=((logname+"_single","event_type text, trace_id bigint, occurrences list<text>, PRIMARY KEY (event_type,trace_id)"))
    tableMap+=((logname+"_lastchecked","event_a text, event_b text, trace_id bigint, timestamp text, PRIMARY KEY ((event_a,event_b),trace_id)"))
    tableMap+=((logname+"_count","event_a text, times list<text>, PRIMARY KEY (event_a)"))
    tableMap+=((logname+"_index","event_a text, event_b text, start timestamp, end timestamp, occurrences list<text>, PRIMARY KEY ((event_a,event_b), start,end)"))
    tableMap.toMap
  }

  /**
   * Returns a map, between an abbreviation of the table name and its actual name. This method assist in making
   * code in [[ApacheCassandraTransformations]] easier to read and understand. The names of the tables (values here)
   * are the same as the keys of the above method
   * @param logname The log database name
   * @return The map between table abbreviations and the actual table names
   */
  def getTableNames(logname:String):Map[String,String]={
    val tableMap:mutable.HashMap[String,String] = new mutable.HashMap[String,String]()
    tableMap+=(("meta",logname+"_meta"))
    tableMap+=(("seq",logname+"_seq"))
    tableMap+=(("single",logname+"_single"))
    tableMap+=(("lastChecked",logname+"_lastchecked"))
    tableMap+=(("count",logname+"_count"))
    tableMap+=(("index",logname+"_index"))
    tableMap.toMap
  }

  /**
   * Contains the mapping between the compression parameters and the compression classes that utilized in Cassandra
   * @param compressionString The compression parameters
   * @return The class that will be utilized by Cassandra for compression
   */
  def getCompression(compressionString:String):String={
    compressionString match {
      case "snappy" => "SnappyCompressor"
      case "lz4" => "LZ4Compressor"
      case "zstd" => "ZstdCompressor"
      case "gzip" => "DeflateCompressor"
      case "uncompressed" => "false"
    }
  }

}
