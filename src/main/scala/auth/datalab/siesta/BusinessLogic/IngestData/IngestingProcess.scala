package auth.datalab.siesta.BusinessLogic.IngestData

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.TraceGenerator.TraceGenerator
import org.apache.spark.rdd.RDD

object IngestingProcess {

  def getData(c:Config): RDD[Structs.Sequence] ={
    if (c.filename != "synthetic") {
      ReadLogFile.readLog(c.filename)
    } else {
      val traceGenerator = new TraceGenerator(c.traces, c.event_types, c.length_min, c.length_max)
      traceGenerator.produce((1 to c.traces).toList)
    }
  }

}
