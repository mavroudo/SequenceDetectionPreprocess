package auth.datalab.siesta.BusinessLogic.IngestData

import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.TraceGenerator.TraceGenerator
import org.apache.spark.rdd.RDD

/**
 * Creates the RDD containing the traces either based on an input file or based on randomly generated traces
 */
object IngestingProcess {

  /**
   * This methods creates the RDD that contains the traces. These traces are either parsed from a log file or generated
   * based on the user define parameters in configuration file.
   * @param c The configuration object containing the command line parameters. Note that in order to activate the
   *          trace generator, the "filename" parameter in the configuration object must be set to "synthetic"
   * @return The RDD that contains the traces
   *
   * @see [[ReadLogFile]],[[auth.datalab.siesta.TraceGenerator.TraceGenerator]] they are responsible for parsing a logfile
   *      or generate random traces respectively.
   */
  def getData(c:Config): RDD[Structs.Sequence] ={
    if (c.filename != "synthetic") {
      ReadLogFile.readLog(c.filename)
    } else {
      val traceGenerator = new TraceGenerator(c.traces, c.event_types, c.length_min, c.length_max)
      traceGenerator.produce((1 to c.traces).toList)
    }
  }

}
