import org.apache.logging.log4j.core.config.composite.MergeStrategy
import sun.security.tools.PathList

name := "Sequence Detection"
version := "0.1"
scalaVersion := "2.11.12"
organization := "auth.datalab.followUps"
parallelExecution in Test := false

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"

//libraryDependencies += "monetdb" % "monetdb-jdbc-new" % "2.36"
//
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

libraryDependencies ++= Seq(
  "org.eu.acolyte" %% "jdbc-scala" % "1.0.46" % "test"
)
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.0.0"

val sparkVersion = "2.4.4"


//to run the sbt assembly the '% "provided",' section must not be in comments
//to debug in IDE the '% "provided",' section must be in comments
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion )
