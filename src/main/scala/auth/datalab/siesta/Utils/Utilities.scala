package auth.datalab.siesta.Utils


import java.sql.Timestamp

object Utilities {


  /**
   * Compare two values of strings. Since the both positions and timestamp can be used, first compare them
   * as if they were integers and if that fails compare them as timestamps.
   *
   * @param timeA first time
   * @param timeB second time
   * @return Return if timeA is less (or before) than timeB
   */
  def compareTimes(timeA: String, timeB: String): Boolean = {
    if (timeA == "") {
      return true
    }
    try {
      timeA.toInt < timeB.toInt
    } catch {
      case _: Throwable =>
        !Timestamp.valueOf(timeA).after(Timestamp.valueOf(timeB))
    }

  }

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
