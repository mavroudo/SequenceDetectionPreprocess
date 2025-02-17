val sparkVersion = "3.5.1"
val hadoopVersion= "3.3.4"
ThisBuild / name := "Siesta"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "auth.datalab"
ThisBuild / Test / parallelExecution := false

assembly / test := {}
<<<<<<< HEAD
assembly / mainClass := Some("auth.datalab.siesta.siesta_main")
=======
assembly / mainClass := Some("auth.datalab.siesta.Main")

>>>>>>> v3.incremental
scalacOptions += "-deprecation"
javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
//scala
libraryDependencies += "de.uni.freiburg.iig.telematik" % "SEWOL" % "1.0.2" //read data
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0" //parser for the commandlines

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided") //% "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion //3.0.3


<<<<<<< HEAD

=======
//
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.14.0"
  )
}

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0"


libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.0" //3.0.3

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-assembly" % "3.2.0"
//delta library
libraryDependencies += "io.delta" %% "delta-core" % "0.8.0"

>>>>>>> v3.incremental

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case manifest if manifest.contains("MANIFEST.MF") =>
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    MergeStrategy.concat
  case deckfour if deckfour.contains("deckfour") || deckfour.contains(".cache") =>
    MergeStrategy.last
  case x =>
    MergeStrategy.last
}
