package auth.datalab.siesta.Utils


import java.sql.Timestamp

object Utilities {

  /**
   * Read environment variable
   *p
   * @param key The key of the variable
   * @return The variable
   * @throws NullPointerException if the variable does not exist
   */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
  }



}
