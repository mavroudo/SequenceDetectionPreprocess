package auth.datalab.siesta.CassandraConnector


import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.ExtractSingle.ExtractSingle
import auth.datalab.siesta.BusinessLogic.Metadata.{MetaData, SetMetadata}
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.Utils.Utilities
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters.mapAsScalaMapConverter


class ApacheCassandraConnector extends DBConnector {
  private var cassandra_host: String = _
  private var cassandra_port: String = _
  private var cassandra_user: String = _
  private var cassandra_pass: String = _
  private var cassandra_replication_class: String = _
  private var cassandra_replication_rack: String = _
  private var cassandra_replication_factor: String = _
  private var cassandra_keyspace_name: String = _
  private var cassandra_write_consistency_level: String = _
  private var cassandra_gc_grace_seconds: String = _
  private var tables: Map[String, String] = Map[String, String]()
  private var _configuration: SparkConf = _
  private val writeConf: WriteConf = WriteConf(consistencyLevel = ConsistencyLevel.LOCAL_ONE) // throughputMiBPS = Option(0.8)

  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  override def initialize_spark(config: Config): Unit = {
    try {
      cassandra_host = Utilities.readEnvVariable("cassandra_host")
      cassandra_port = Utilities.readEnvVariable("cassandra_port")
      cassandra_user = Utilities.readEnvVariable("cassandra_user")
      cassandra_pass = Utilities.readEnvVariable("cassandra_pass")
      cassandra_gc_grace_seconds = Utilities.readEnvVariable("cassandra_gc_grace_seconds")
      cassandra_keyspace_name = Utilities.readEnvVariable("cassandra_keyspace_name")
      cassandra_replication_class = Utilities.readEnvVariable("cassandra_replication_class")
      cassandra_replication_rack = Utilities.readEnvVariable("cassandra_replication_rack")
      cassandra_replication_factor = Utilities.readEnvVariable("cassandra_replication_factor")
      cassandra_write_consistency_level = Utilities.readEnvVariable("cassandra_write_consistency_level")
      //      println(cassandra_host, cassandra_keyspace_name, cassandra_keyspace_name)
    } catch {
      case e: NullPointerException =>
        e.printStackTrace()
        System.exit(1)
    }
    _configuration = new SparkConf()
      .setAppName("SIESTA indexing")
//      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandra_host)
      .set("spark.cassandra.auth.username", cassandra_user)
      .set("spark.cassandra.auth.password", cassandra_pass)
      .set("spark.cassandra.connection.port", cassandra_port)
      .set("spark.cassandra.output.consistency.level", cassandra_write_consistency_level)
//      .set("spark.cassandra.connection.timeoutMS", "20000")
      .set("spark.cassandra.input.throughputMBPerSec","1")

    val spark = SparkSession.builder().config(_configuration).getOrCreate()


  }

  /**
   * Create the appropriate tables, remove previous ones
   */
  override def initialize_db(config: Config): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.execute("create keyspace if not exists " + cassandra_keyspace_name + " WITH replication = "
          + "{'class':'" + cassandra_replication_class + "', '" + cassandra_replication_rack + "':" + cassandra_replication_factor + "}")
        session.execute("USE " + cassandra_keyspace_name + ";")
      }
    } catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred creating the keyspace")
        print(e.printStackTrace())
        spark.close() //Stop Spark
        System.exit(1)
    }
    this.tables = CassandraTables.getTableNames(config.log_name)
    if (config.delete_previous) {
      this.dropTables(CassandraTables.getTablesStructures(config.log_name).keys.toList)
    }
    if (config.delete_all) {
      this.dropAlltables()
    }

    this.createTables(CassandraTables.getTablesStructures(config.log_name))
    this.setCompression(CassandraTables.getTablesStructures(config.log_name).keys.toList, CassandraTables.getCompression(config.compression))

  }

  private def dropAlltables(): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        session.getMetadata
          .getKeyspace(this.cassandra_keyspace_name)
          .get().getTables.asScala
          .map(x => x._2.getName.toString)
          .foreach(table => {
            session.execute("drop table if exists " + this.cassandra_keyspace_name + '.' + table + ";")
          })
        session.close()
      }
    } catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred dropping tables tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  private def dropTables(tables: List[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- tables) {
          session.execute("DROP TABLE IF EXISTS " + cassandra_keyspace_name + "." + table + ";")
        }
      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred dropping tables tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  private def createTables(names: Map[String, String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
        for (table <- names) {
          session.execute(s"CREATE TABLE IF NOT EXISTS $cassandra_keyspace_name.${table._1} (${table._2}) " +
            s"WITH GC_GRACE_SECONDS=$cassandra_gc_grace_seconds")
        }
      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred creating the tables")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  private def setCompression(tables: List[String], compression: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      if (compression != "false") {
        CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
          for (table <- tables) {
            session.execute(s"ALTER TABLE $cassandra_keyspace_name.$table " +
              s"WITH compression={'class': '$compression'};")
          }
        }
      } else {
        CassandraConnector(spark.sparkContext.getConf).withSessionDo { session =>
          for (table <- tables) {
            session.execute(s"ALTER TABLE $cassandra_keyspace_name.$table " +
              s"WITH compression={'enabled': 'false'};")
          }
        }
      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred when setting compression")
        e.printStackTrace()
        spark.close()
        System.exit(1)
    }
  }

  /**
   * This method constructs the appropriate metadata based on the already stored in the database and the
   * new presented in the config object
   *
   * @param config contains the configuration passed during execution
   * @return the metadata
   */
  override def get_metadata(config: Config): MetaData = {
    Logger.getLogger("Metadata").log(Level.INFO, s"Getting metadata")
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().getOrCreate()
    val prevMetaData = spark.read.cassandraFormat(tables("meta"), this.cassandra_keyspace_name, "").load()
    var metaData:MetaData=null
    try {
        metaData = if (prevMetaData.count() == 0) { //has no previous record
          SetMetadata.initialize_metadata(config)
        } else {
          this.load_metadata(prevMetaData, config)
        }
    } catch{
    case _:java.util.NoSuchElementException  => metaData = SetMetadata.initialize_metadata(config)
  }
    val total = System.currentTimeMillis() - start
    this.write_metadata(metaData) //persist this version back
    Logger.getLogger("Metadata").log(Level.INFO, s"finished in ${total / 1000} seconds")
    metaData
  }

  private def load_metadata(meta: DataFrame, config: Config): MetaData = {
    val m = meta.collect().map(r => (r.getAs[String]("key"), r.getAs[String]("value"))).toMap
    MetaData(traces = m("traces").toInt, events = m("events").toInt, pairs = m("pairs").toInt, lookback = m("lookback").toInt,
      split_every_days = m("split_every_days").toInt, last_interval = m("last_interval"),
      has_previous_stored = m("has_previous_stored").toBoolean, filename = m("filename"),
      log_name = m("log_name"), mode = m("mode"), compression = m("compression"))
  }

  /**
   * Persists metadata
   *
   * @param metaData metadata of the execution and the database
   */
  override def write_metadata(metaData: MetaData): Unit = {
    val df = ApacheCassandraTransformations.transformMetaToDF(metaData)
    df.write.mode(SaveMode.Overwrite)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tables("meta"), "keyspace" -> this.cassandra_keyspace_name, "confirm.truncate" -> "true"))
      .save()
  }

  /**
   * Read data as an rdd from the SeqTable
   *
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  override def read_sequence_table(metaData: MetaData): RDD[Structs.Sequence] = {
    val df = this.readTable(tables("seq"))
    if (df.isEmpty) {
      return null
    }
    val rdd = ApacheCassandraTransformations.transformSeqToRDD(df)
    rdd
  }



  /**
   * This method writes traces to the auxiliary SeqTable. Since RDD will be used as intermediate results it is already persisted
   * and should not be modify that.
   * If states in the metadata, this method should combine the new traces with the previous ones
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   *
   * @param sequenceRDD RDD containing the traces
   * @param metaData    Containing all the necessary information for the storing
   */
  override def write_sequence_table(sequenceRDD: RDD[Structs.Sequence], metaData: MetaData): RDD[Structs.LastPosition] = {
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"Start writing sequence table")
    val start = System.currentTimeMillis()
//    val combined = this.combine_sequence_table(sequenceRDD, prevSeq)
    val rddCass = ApacheCassandraTransformations.transformSeqToWrite(sequenceRDD)
    rddCass.persist(StorageLevel.MEMORY_AND_DISK)
    metaData.traces = rddCass.count()
    rddCass
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("seq"),
        columns = SomeColumns("events" append, "sequence_id"), writeConf = writeConf)
    rddCass.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    this.read_sequence_table(metaData).map(s=>Structs.LastPosition(s.sequence_id,s.events.size))
  }

  private def readTable(name: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val table = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> name,
        "keyspace" -> cassandra_keyspace_name.toLowerCase()
      ))
      .load()
    table
  }

  /**
   * This method writes traces to the auxiliary SingleTable. The rdd that comes to this method is not persisted.
   * Database should persist it before store it and not persist it at the end.
   * This method should combine the results with previous ones and return the results to the main pipeline
   * Additionally updates metaData object
   *
   * @param singleRDD Contains the single inverted index
   * @param metaData  Containing all the necessary information for the storing
   */
  override def write_single_table(singleRDD: RDD[Structs.InvertedSingleFull], metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    Logger.getLogger("Single Table Write").log(Level.INFO, s"Start writing single table")
    val start = System.currentTimeMillis()
    val newEvents = singleRDD.map(x => x.times.size).reduce((x, y) => x + y)
    metaData.events += newEvents //count and update metadata
    val previousSingle = read_single_table(metaData)
    val combined = combine_single_table(singleRDD, previousSingle)
    val transformed = ApacheCassandraTransformations.transformSingleToWrite(singleRDD)
    transformed.persist(StorageLevel.MEMORY_AND_DISK)
    transformed
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("single"),
        columns = SomeColumns("event_type", "trace_id", "occurrences" append), writeConf = writeConf)
    transformed.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Single Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    combined
  }

  override def combine_single_table(newSingle: RDD[Structs.InvertedSingleFull], previousSingle: RDD[Structs.InvertedSingleFull]): RDD[Structs.InvertedSingleFull] = {
    if (previousSingle == null) return newSingle
    val combined = previousSingle.keyBy(x=>(x.id,x.event_name)).fullOuterJoin(newSingle.keyBy(x=>(x.id,x.event_name)))
      .map(x=>{
        val previous = x._2._1.getOrElse(Structs.InvertedSingleFull(-1, "", List(), List()))
        val prevOc = previous.times.zip(previous.positions)
        val newly = x._2._2.getOrElse(Structs.InvertedSingleFull(-1, "", List(), List()))
        val newOc = newly.times.zip(newly.positions)
        val combine = (prevOc++newOc).distinct
        val event = if (previous.event_name=="") newly.event_name else previous.event_name
        Structs.InvertedSingleFull(x._1._1, event, combine.map(_._1), combine.map(_._2))
      })
    combined
  }


  /**
   * Read data as an rdd from the SingleTable
   *
   * @param metaData Containing all the necessary information for the storing
   * @return In RDD the stored data
   */
  override def read_single_table(metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    val df = this.readTable(tables("single"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformSingleToRDD(df)
  }

  /**
   * Returns data from LastChecked Table
   *
   * @param metaData Containing all the necessary information for the storing
   * @return LastChecked records
   */
  override def read_last_checked_table(metaData: MetaData): RDD[Structs.LastChecked] = {
    val df = this.readTable(tables("lastChecked"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformLastCheckedToRDD(df)
  }

  /**
   * Writes new records for last checked back in the database and return the combined records with the
   *
   * @param lastChecked records containing the timestamp of last completion for each different n-tuple
   * @param metaData    Containing all the necessary information for the storing
   * @return The combined last checked records
   */
  override def write_last_checked_table(lastChecked: RDD[Structs.LastChecked], metaData: MetaData)= {
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"Start writing LastChecked table")
    val start = System.currentTimeMillis()
    val transformed = ApacheCassandraTransformations.transformLastCheckedToWrite(lastChecked)
    transformed.persist(StorageLevel.MEMORY_AND_DISK)
    transformed
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("lastChecked"),
        writeConf = writeConf)
    transformed.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("LastChecked Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Read data previously stored data that correspond to the intervals, in order to be merged
   *
   * @param metaData  Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return combined record of pairs during the interval periods
   */
  override def read_index_table(metaData: MetaData, intervals: List[Structs.Interval]): RDD[Structs.PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    val df = this.readTable(tables("index"))
    df.createOrReplaceTempView("IndexTable")
    val interval_min = intervals.map(_.start).distinct.sortWith((x, y) => x.before(y)).head
    val interval_max = intervals.map(_.end).distinct.sortWith((x, y) => x.before(y)).last
    val parkSQL = spark.sql(s"""select * from IndexTable where (start>=to_timestamp('$interval_min') and end<=to_timestamp('$interval_max'))""")
    if (parkSQL.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformIndexToRDD(parkSQL, metaData)
  }

  /**
   * Reads the all the indexed pairs (mainly for testing reasons) advice to use the above method
   *
   * @param metaData Containing all the necessary information for the storing
   * @return All the indexed pairs
   */
  override def read_index_table(metaData: MetaData): RDD[Structs.PairFull] = {
    val df = this.readTable(tables("index"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformIndexToRDD(df, metaData)
  }

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   *
   * @param newPairs  The newly generated pairs
   * @param metaData  Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted
   */
  override def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData, intervals: List[Structs.Interval]): Unit = {
    Logger.getLogger("Index Table Write").log(Level.INFO, s"Start writing Index table")
    val start = System.currentTimeMillis()
    //    val previousIndexed = this.read_index_table(metaData, intervals)
    //    val combined = this.combine_index_table(newPairs, previousIndexed, metaData, intervals)
    metaData.pairs += newPairs.count()
    val df = ApacheCassandraTransformations.transformIndexToWrite(newPairs, metaData)
    df.persist(StorageLevel.MEMORY_AND_DISK)
    df
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("index"),
        columns = SomeColumns("event_a", "event_b", "start", "end", "occurrences" append), writeConf = writeConf)
    df.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Index Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Read previously stored data in the count table
   *
   * @param metaData Containing all the necessary information for the storing
   * @return The count data stored in the count table
   */
  override def read_count_table(metaData: MetaData): RDD[Structs.Count] = {
    val df = this.readTable(tables("count"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformCountToRDD(df)
  }

  /**
   * Write count to countTable
   *
   * @param counts   Calculated basic statistics in order to be stored in the count table
   * @param metaData Containing all the necessary information for the storing
   */
  override def write_count_table(counts: RDD[Structs.Count], metaData: MetaData): Unit = {
    Logger.getLogger("Count Table Write").log(Level.INFO, s" writing Count table")
    val start = System.currentTimeMillis()
    val previousIndexed = this.read_count_table(metaData)
    val combined = this.combine_count_table(counts, previousIndexed, metaData)
    val df = ApacheCassandraTransformations.transformCountToWrite(combined)
    df.persist(StorageLevel.MEMORY_AND_DISK)
    df
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("count"), writeConf = writeConf)
    df.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Count Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

}
