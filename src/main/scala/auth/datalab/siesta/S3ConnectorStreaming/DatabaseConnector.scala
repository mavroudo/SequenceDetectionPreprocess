package auth.datalab.siesta.S3ConnectorStreaming

import auth.datalab.siesta.Utils.Utilities

import java.sql.{Connection, DriverManager, SQLException, Statement}

object DatabaseConnector {
  //  val url = "jdbc:postgresql://localhost:5432/metrics"
  //  val username = "admin"
  //  val password = "admin"

  private val endpoint: String = Utilities.readEnvVariable("POSTGRES_ENDPOINT")
  val url = s"jdbc:postgresql://$endpoint"
  val username: String = Utilities.readEnvVariable("POSTGRES_USERNAME")
  val password: String = Utilities.readEnvVariable("POSTGRES_PASSWORD")

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
        |    id  SERIAL PRIMARY KEY,
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

  def createCustomStateTableIfNotExists(connection: Connection): Unit = {
    val createTableSQL =
      """
        |CREATE TABLE IF NOT EXISTS custom_state (
        |    id  SERIAL PRIMARY KEY,
        |    number_of_events BIGINT
        |);
        |""".stripMargin

    val createTable2SQL =
      """
        |CREATE TABLE IF NOT EXISTS custom_state_events (
        |    id  SERIAL PRIMARY KEY,
        |    state_id INT REFERENCES custom_state(id) ON DELETE CASCADE,
        |    event_key VARCHAR(100),
        |    event_timestamp TIMESTAMP,
        |    event_value INT
        |);
        |""".stripMargin

    var statement: Statement = null
    try {
      statement = connection.createStatement()
      statement.execute(createTableSQL)
      statement.execute(createTable2SQL)
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (statement != null) statement.close()
    }
  }

  def createCountStateTableIfNotExists(connection: Connection): Unit = {
    val createTableSQL =
      """
        |CREATE TABLE IF NOT EXISTS count_state (
        |    id SERIAL PRIMARY KEY,
        |    event_pair TEXT UNIQUE,
        |    sum_duration BIGINT,
        |    count INT,
        |    min_duration BIGINT,
        |    max_duration BIGINT
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
