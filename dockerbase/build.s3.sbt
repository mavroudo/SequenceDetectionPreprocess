val sparkVersion = "2.4.4"

ThisBuild / name := "Siesta"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "auth.datalab"
ThisBuild / parallelExecution in Test := false
test in assembly := {}
assembly / mainClass := Some("auth.datalab.siesta.Main")

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"
//test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
//scala
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.6"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "de.uni.freiburg.iig.telematik" % "SEWOL" % "1.0.2"

libraryDependencies += "org.eu.acolyte" %% "jdbc-scala" % "1.0.46" % "test"
libraryDependencies += "com.madhukaraphatak" %% "java-sizeof" % "0.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"
//to run the sbt assembly the '% "provided",' section must not be in comments
//to debug in IDE the '  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion , //"2.0.0",' section must be in comments
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion % "provided", //"2.0.0"
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided")

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}


libraryDependencies += "io.minio" % "spark-select_2.11" % "2.1"
libraryDependencies += "io.minio" % "minio" % "3.0.12"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.0.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.0.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.0.3"


assemblyMergeStrategy in assembly := {
  case manifest if manifest.contains("MANIFEST.MF") =>
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    MergeStrategy.concat
  case deckfour if deckfour.contains("deckfour") || deckfour.contains(".cache") =>
    MergeStrategy.last
  case x =>
    MergeStrategy.last
}
