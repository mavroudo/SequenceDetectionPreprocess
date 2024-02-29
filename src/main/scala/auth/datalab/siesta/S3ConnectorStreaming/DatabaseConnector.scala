package auth.datalab.siesta.S3ConnectorStreaming
import java.sql.{Connection, DriverManager, SQLException, Statement}

object DatabaseConnector {
  val url = "jdbc:postgresql://localhost:5432/metrics"
  val username = "admin"
  val password = "admin"

  def getConnection: Connection = {
    try {
      Class.forName("org.postgresql.Driver")
      DriverManager.getConnection(url, username, password)
    } catch {
      case e: ClassNotFoundException => throw new IllegalStateException("PostgreSQL driver not found.", e)
      case e: SQLException => throw new IllegalStateException("Cannot connect to the database.", e)
    }
  }

  def createTableIfNotExists(connection: Connection): Unit = {
    val createTableSQL =
      """
        |CREATE TABLE IF NOT EXISTS logs (
        |    query_name VARCHAR(255),
        |    ts BIGINT,
        |    batch_id BIGINT,
        |    batch_duration BIGINT,
        |    num_input_rows BIGINT
        |);
        |""".stripMargin

    var statement: Statement = null
    try {
      statement = connection.createStatement()
      statement.execute(createTableSQL)
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (statement != null) statement.close()
    }
  }


}
