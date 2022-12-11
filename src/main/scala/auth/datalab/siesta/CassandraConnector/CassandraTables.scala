package auth.datalab.siesta.CassandraConnector

import scala.collection.mutable

object CassandraTables {

  def getTablesStructures(logname:String):Map[String,String]={
    val tableMap:mutable.HashMap[String,String] = new mutable.HashMap[String,String]()
    tableMap+=((logname+"_meta","key text, value text, PRIMARY KEY (key)"))
    tableMap+=((logname+"_seq","sequence_id text, events list<text>, PRIMARY KEY (sequence_id)"))
    tableMap+=((logname+"_single","event_type text, occurrences list<text>, PRIMARY KEY (event_type)"))
    tableMap+=((logname+"_lastchecked","event_a text, event_b text, occurrences list<text>, PRIMARY KEY (event_a,event_b)"))
    tableMap+=((logname+"_count","event_a text, times list<text>, PRIMARY KEY (event_a)"))
    tableMap.toMap
  }

  def getTableNames(logname:String):Map[String,String]={
    val tableMap:mutable.HashMap[String,String] = new mutable.HashMap[String,String]()
    tableMap+=(("meta",logname+"_meta"))
    tableMap+=(("seq",logname+"_seq"))
    tableMap+=(("single",logname+"_single"))
    tableMap+=(("lastChecked",logname+"_lastchecked"))
    tableMap+=(("count",logname+"_count"))
    tableMap.toMap
  }

}
