val sparkVersion = "3.5.4"
val hadoopVersion= "3.3.4"

ThisBuild / name := "Siesta"
ThisBuild / version := "3.0.0"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "auth.datalab"

ThisBuild / Test / parallelExecution := false

test in assembly := {}

assembly / mainClass := Some("auth.datalab.siesta.siesta_main")

scalacOptions += "-deprecation"
javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
//scala
libraryDependencies += "de.uni.freiburg.iig.telematik" % "SEWOL" % "1.0.2" //read data
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0" //parser for the commandlines

//to run the sbt assembly the '% "provided",' section must not be in comments
//to debug in IDE the '  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion , //"2.0.0",' section must be in comments
libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-sql" % sparkVersion )
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion //3.0.3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"


libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
//libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"

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
