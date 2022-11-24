package auth.datalab.siesta.Utils

import auth.datalab.siesta.CommandLineParser.Config
import auth.datalab.siesta.TraceGenerator.TraceGenerator

import java.sql.Timestamp

object Utilities {


  /**
   * Return if a is less than b
   *
   * @param timeA first time
   * @param timeB second time
   * @return
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
   * Method for sorting entries based on their timestamps
   *
   * @param s1 Timestamp in string format
   * @param s2 Timestamp in string format
   * @return True or false based on order
   */
  def sortByTime(s1: String, s2: String): Boolean = {
    val time1 = Timestamp.valueOf(s1)
    val time2 = Timestamp.valueOf(s2)
    time1.before(time2)
  }

  /**
   * Method to return the difference in milliseconds between timestamps
   *
   * @param pr_time  The 1st timestamp in string format
   * @param new_time The 2nd timestamp in string format
   * @return The difference in long format
   */
  def getDifferenceTime(pr_time: String, new_time: String): Long = {
    val time_pr = Timestamp.valueOf(pr_time)
    val time_new = Timestamp.valueOf(new_time)
    val res = Math.abs(time_new.getTime - time_pr.getTime)
    res
  }

  /**
   * Method to read environment variables
   *
   * @param key The key of the variable
   * @return The variable
   */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
  }



}
