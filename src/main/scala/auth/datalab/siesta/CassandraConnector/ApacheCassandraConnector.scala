package auth.datalab.siesta.CassandraConnector

import auth.datalab.sequenceDetection.Utils
import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
import auth.datalab.siesta.BusinessLogic.Metadata.{MetaData, SetMetadata}
import auth.datalab.siesta.BusinessLogic.Model.Structs
import auth.datalab.siesta.CommandLineParser.Config
import com.datastax.driver.core.{Cluster, ConsistencyLevel}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.net.InetSocketAddress

class ApacheCassandraConnector extends DBConnector {
  var cassandra_host: String = _
  var cassandra_port: String = _
  var cassandra_user: String = _
  var cassandra_pass: String = _
  var cassandra_replication_class: String = _
  var cassandra_replication_rack: String = _
  var cassandra_replication_factor: String = _
  var cassandra_keyspace_name: String = _
  var cassandra_write_consistency_level: String = _
  var cassandra_gc_grace_seconds: String = _
  var tables: Map[String, String] = Map[String, String]()
  var _configuration: SparkConf = _
  val DELIMITER = "¦delab¦"
  val writeConf: WriteConf = WriteConf(consistencyLevel = ConsistencyLevel.ONE, batchSize = 1, throughputMiBPS = 0.5)

  /**
   * Depending on the different database, each connector has to initialize the spark context
   */
  override def initialize_spark(config: Config): Unit = {
    try {
      cassandra_host = Utils.readEnvVariable("cassandra_host")
      cassandra_port = Utils.readEnvVariable("cassandra_port")
      cassandra_user = Utils.readEnvVariable("cassandra_user")
      cassandra_pass = Utils.readEnvVariable("cassandra_pass")
      cassandra_gc_grace_seconds = Utils.readEnvVariable("cassandra_gc_grace_seconds")
      cassandra_keyspace_name = Utils.readEnvVariable("cassandra_keyspace_name")
      cassandra_replication_class = Utils.readEnvVariable("cassandra_replication_class")
      cassandra_replication_rack = Utils.readEnvVariable("cassandra_replication_rack")
      cassandra_replication_factor = Utils.readEnvVariable("cassandra_replication_factor")
      cassandra_write_consistency_level = Utils.readEnvVariable("cassandra_write_consistency_level")
      //      println(cassandra_host, cassandra_keyspace_name, cassandra_keyspace_name)
    } catch {
      case e: NullPointerException =>
        e.printStackTrace()
        System.exit(1)
    }
    _configuration = new SparkConf()
      .setAppName("FA Indexing")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandra_host)
      .set("spark.cassandra.auth.username", cassandra_user)
      .set("spark.cassandra.auth.password", cassandra_pass)
      .set("spark.cassandra.connection.port", cassandra_port)
      .set("spark.cassandra.output.consistency.level", cassandra_write_consistency_level)

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

  }

  private def dropAlltables(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    try {
      val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(this.cassandra_host, this.cassandra_port.toInt)).withCredentials(this.cassandra_user, this.cassandra_pass).build()
      val session = cluster.connect(this.cassandra_keyspace_name)
      val tables_iterator = cluster.getMetadata.getKeyspace(this.cassandra_keyspace_name).getTables.iterator()
      while (tables_iterator.hasNext) {
        session.execute("drop table if exists " + this.cassandra_keyspace_name + '.' + tables_iterator.next.getName + ";")
      }
      session.close()
      cluster.close()

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
          session.execute("CREATE TABLE IF NOT EXISTS " + cassandra_keyspace_name + "." +
            table._1 + " (" + table._2 + ") " +
            "WITH GC_GRACE_SECONDS=" + cassandra_gc_grace_seconds +
            ";")
        }
      }
    }
    catch {
      case e: Exception =>
        Logger.getLogger("Initialization db").log(Level.ERROR, s"A problem occurred creating the tables")
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
    val metaData = if (prevMetaData.count() == 0) { //has no previous record
      SetMetadata.initialize_metadata(config)
    } else {
      this.load_metadata(prevMetaData, config)
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
  override def write_sequence_table(sequenceRDD: RDD[Structs.Sequence], metaData: MetaData): RDD[Structs.Sequence] = {
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"Start writing sequence table")
    val start = System.currentTimeMillis()
    val prevSeq = this.read_sequence_table(metaData)
    val combined = this.combine_sequence_table(sequenceRDD, prevSeq)
    val rddCass = ApacheCassandraTransformations.transformSeqToWrite(combined)
    rddCass.persist()
    rddCass
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("seq"),
        columns = SomeColumns("events", "sequence_id"), writeConf = writeConf)
    rddCass.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    combined
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
    val transformed = ApacheCassandraTransformations.transformSingleToWrite(combined)
    transformed.persist(StorageLevel.MEMORY_AND_DISK)
    transformed
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("single"),
        columns = SomeColumns("event_type", "occurrences"), writeConf = writeConf)
    transformed.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Single Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
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
  override def read_last_checked_table(metaData: MetaData): RDD[Structs.LastChecked] = ???

  /**
   * Writes new records for last checked back in the database and return the combined records with the
   *
   * @param lastChecked records containing the timestamp of last completion for each different n-tuple
   * @param metaData    Containing all the necessary information for the storing
   * @return The combined last checked records
   */
  override def write_last_checked_table(lastChecked: RDD[Structs.LastChecked], metaData: MetaData): RDD[Structs.LastChecked] = ???

  /**
   * Read data previously stored data that correspond to the intervals, in order to be merged
   *
   * @param metaData  Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return combined record of pairs during the interval periods
   */
  override def read_index_table(metaData: MetaData, intervals: List[Structs.Interval]): RDD[Structs.PairFull] = ???

  /**
   * Reads the all the indexed pairs (mainly for testing reasons) advice to use the above method
   *
   * @param metaData Containing all the necessary information for the storing
   * @return All the indexed pairs
   */
  override def read_index_table(metaData: MetaData): RDD[Structs.PairFull] = ???

  /**
   * Write the combined pairs back to the S3, grouped by the interval and the first event
   *
   * @param newPairs  The newly generated pairs
   * @param metaData  Containing all the necessary information for the storing
   * @param intervals The period of times that the pairs will be splitted
   */
  override def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData, intervals: List[Structs.Interval]): Unit = ???

  /**
   * Read previously stored data in the count table
   *
   * @param metaData Containing all the necessary information for the storing
   * @return The count data stored in the count table
   */
  override def read_count_table(metaData: MetaData): RDD[Structs.Count] = ???

  /**
   * Write count to countTable
   *
   * @param counts   Calculated basic statistics in order to be stored in the count table
   * @param metaData Containing all the necessary information for the storing
   */
  override def write_count_table(counts: RDD[Structs.Count], metaData: MetaData): Unit = ???
}
