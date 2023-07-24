package auth.datalab.siesta.CassandraConnector


import auth.datalab.siesta.BusinessLogic.DBConnector.DBConnector
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
import scala.language.postfixOps


/**
 * This class handles the communication between between SIESTA and Cassandra. It inherits all the signatures of the methods
 * from [[auth.datalab.siesta.BusinessLogic.DBConnector]] and overrides the reads and writes to match Cassandra properties.
 * Cassandra is a distributed key-value database. Therefore each record should have a key (either single or complex key).
 *
 * @see [[CassandraTables]], [[ApacheCassandraTransformations]], where the structure of the tables is described in the first
 *      and the transformations between RDDs and Dataframes in the second.
 */
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
   * Spark initilizes the connection to Cassandra utilizing cassandra properties, that are available through the
   * cassandra-connector library.
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
      .set("spark.cassandra.connection.timeoutMS", "20000")
//      .set("spark.cassandra.input.throughputMBPerSec","1")

    SparkSession.builder().config(_configuration).getOrCreate()


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

  /**
   * Drop all the tables in the currect keyspace in Cassandra. It will delete all the tables despite the log
   * database name.
   */
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

  /**
   * This method is used to delete all the previous tables of the current log database. Therefore, it will search
   * each table from the list in the current keyspace and remove any tables found.
   * @param tables The list with the table names
   */
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

  /**
   * Create the tables that are passed in the parameter. Each table is a key-> value in the map, where key is its name
   * and value is the table's structure. This information is available in the [[CassandraTables]] class, where the names
   * are generated for a given log database name.
   * @param names
   */
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

  /**
   * This method sets the compression that will be used in every table that is present in the provided list.
   *
   * @param tables The name of the tables where the compression algorithm will be set.
   * @param compression The compression algorithm that will be set to all the tables.
   * @see [[CassandraTables]] which describes the match between compression algorithm and compression class
   */
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
          this.load_metadata(prevMetaData)
        }
    } catch{
    case _:java.util.NoSuchElementException  => metaData = SetMetadata.initialize_metadata(config)
  }
    val total = System.currentTimeMillis() - start
    this.write_metadata(metaData) //persist this version back
    Logger.getLogger("Metadata").log(Level.INFO, s"finished in ${total / 1000} seconds")
    metaData
  }

  /**
   * Extracts the metadata object from the Dataframe retrieved from Cassandra
   * @param meta The Dataframe from Cassandra
   * @return The metadata object
   */
  private def load_metadata(meta: DataFrame): MetaData = {
    val m = meta.collect().map(r => (r.getAs[String]("key"), r.getAs[String]("value"))).toMap
    MetaData(traces = m("traces").toInt, events = m("events").toInt, pairs = m("pairs").toInt, lookback = m("lookback").toInt,
      split_every_days = m("split_every_days").toInt, last_interval = m("last_interval"),
      has_previous_stored = m("has_previous_stored").toBoolean, filename = m("filename"),
      log_name = m("log_name"), mode = m("mode"), compression = m("compression"), last_checked_split = m("last_checked_split").toInt)
  }

  /**
   * Persists metadata
   *
   * @param metaData Object containing the metadata
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
   * @param metaData Object containing the metadata
   * @return Object containing the metadata
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
   * and should not be modified.
   * This method should combine the results with previous ones and return them to the main pipeline.
   * Additionally updates metaData object
   *
   * @param sequenceRDD The RDD containing the traces
   * @param metaData    Object containing the metadata
   * @return An RDD with the last position of the event stored per trace
   */
  override def write_sequence_table(sequenceRDD: RDD[Structs.Sequence], metaData: MetaData): RDD[Structs.LastPosition] = {
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"Start writing sequence table")
    val start = System.currentTimeMillis()
    val rddCass = ApacheCassandraTransformations.transformSeqToWrite(sequenceRDD)
    rddCass.persist(StorageLevel.MEMORY_AND_DISK)
    metaData.traces += rddCass.count()
    rddCass
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("seq"),
        columns = SomeColumns("events" append, "sequence_id"), writeConf = writeConf)
    rddCass.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Sequence Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    this.read_sequence_table(metaData).map(s=>Structs.LastPosition(s.sequence_id,s.events.size))
  }

  /**
   * Generic method that loads data from a specific table in Cassandra to a Dataframe.
   * @param name The name of the table.
   * @return The Dataframe with the loaded data.
   */
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
   * Database should persist it before store it and unpersist it at the end.
   * This method should combine the results with the ones previously stored and return them to the main pipeline.
   * Additionally updates metaData object.
   *
   * @param singleRDD Contains the newly indexed events in a form of single inverted index
   * @param metaData  Object containing the metadata
   */
  override def write_single_table(singleRDD: RDD[Structs.InvertedSingleFull], metaData: MetaData): RDD[Structs.InvertedSingleFull] = {
    Logger.getLogger("Single Table Write").log(Level.INFO, s"Start writing single table")
    val start = System.currentTimeMillis()
    val newEvents = singleRDD.map(x => x.times.size).reduce((x, y) => x + y)
    val new_traces = singleRDD.map(_.id).distinct().collect().toSet
    metaData.events += newEvents //count and update metadata
    val previousSingle = read_single_table(metaData)
    val combined = combine_single_table(singleRDD, previousSingle)
    combined.persist(StorageLevel.MEMORY_AND_DISK)
    val transformed = ApacheCassandraTransformations.transformSingleToWrite(singleRDD)
//    transformed.persist(StorageLevel.MEMORY_AND_DISK)
    transformed
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("single"),
        columns = SomeColumns("event_type", "trace_id", "occurrences" append), writeConf = writeConf)
//    transformed.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Single Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
    combined.filter(x=>new_traces.contains(x.id))
  }


  /**
   * Loads the single inverted index from Cassandra, stored in the SingleTable
   *
   * @param metaData Object containing the metadata
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
   * Loads data from the LastChecked Table, which contains the  information of the last timestamp per event type pair
   * per trace.
   *
   * @param metaData Object containing the metadata
   * @return An RDD with the last timestamps per event type pair per trace
   */
  override def read_last_checked_table(metaData: MetaData): RDD[Structs.LastChecked] = {
    val df = this.readTable(tables("lastChecked"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformLastCheckedToRDD(df)
  }

  override def read_last_checked_partitioned_table(metaData: MetaData, partitions: List[Long]): RDD[Structs.LastChecked] = {
    //TODO: see if need this to be fixed with partitions like s3
    read_last_checked_table(metaData)
  }

  /**
   * Stores new records for last checked back in the database
   *
   * @param lastChecked Records containing the timestamp of last completion for each event type pair for each trace
   * @param metaData    Object containing the metadata
   */
  override def write_last_checked_table(lastChecked: RDD[Structs.LastChecked], metaData: MetaData):Unit= {
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
   * Loads data from the IndexTable that correspond to the given intervals. These data will be merged with
   * the newly calculated pairs, before written back to the database.
   *
   * @param metaData  Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted (thus require to combine with previous pairs,
   *                  if there are any in these periods)
   * @return Loaded records from IndexTable for the given intervals
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
   * Loads all the indexed pairs from the IndexTable. Mainly used for testing reasons.
   * Advice: use the above method.
   *
   * @param metaData Object containing the metadata
   * @return Loaded indexed pairs from IndexTable
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
   * @param metaData  Object containing the metadata
   * @param intervals The period of times that the pairs will be splitted
   */
  override def write_index_table(newPairs: RDD[Structs.PairFull], metaData: MetaData, intervals: List[Structs.Interval]): Unit = {
    Logger.getLogger("Index Table Write").log(Level.INFO, s"Start writing Index table")
    val start = System.currentTimeMillis()
    metaData.pairs += newPairs.count()
    val df = ApacheCassandraTransformations.transformIndexToWrite(newPairs, metaData)
    df.persist(StorageLevel.MEMORY_AND_DISK)
    //utilize the append property to make it more efficient than merging them
    df
      .saveToCassandra(keyspaceName = this.cassandra_keyspace_name, tableName = this.tables("index"),
        columns = SomeColumns("event_a", "event_b", "start", "end", "occurrences" append), writeConf = writeConf)
    df.unpersist()
    val total = System.currentTimeMillis() - start
    Logger.getLogger("Index Table Write").log(Level.INFO, s"finished in ${total / 1000} seconds")
  }

  /**
   * Loads previously stored data in the CountTable
   *
   * @param metaData Object containing the metadata
   * @return The data stored in the count table
   */
  override def read_count_table(metaData: MetaData): RDD[Structs.Count] = {
    val df = this.readTable(tables("count"))
    if (df.isEmpty) {
      return null
    }
    ApacheCassandraTransformations.transformCountToRDD(df)
  }

  /**
   * Writes count to countTable
   *
   * @param counts   Calculated basic statistics per event type pair in order to be stored in the count table
   * @param metaData Object containing the metadata
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
