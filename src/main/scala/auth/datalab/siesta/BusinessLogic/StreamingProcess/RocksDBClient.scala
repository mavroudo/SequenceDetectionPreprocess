package auth.datalab.siesta.BusinessLogic.StreamingProcess

import org.rocksdb.{RocksDB, Options}

object RocksDBClient {
  private var db: RocksDB = _

  def init(): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true)
      db = RocksDB.open(options, "/Users/cmoutafidis/Documents/projects/dws/mavroudo/SequenceDetectionPreprocess/rocksdb")
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw new IllegalStateException("Failed to initialize RocksDB", e)
    }
  }

  def getState(key: String): Option[Array[Byte]] = {
    try {
      Option(db.get(key.getBytes))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  def saveState(key: String, state: Array[Byte]): Unit = {
    try {
      db.put(key.getBytes, state)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def close(): Unit = {
    if (db != null) {
      db.close()
    }
  }

  // Ensure the database is initialized when the object is accessed for the first time
  init()
}
