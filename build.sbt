
name := "SequenceDetection"
version := "0.1"
scalaVersion := "2.11.12"
organization := "auth.datalab"
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

val sparkVersion = "2.4.4"


//to run the sbt assembly the '% "provided",' section must not be in comments
//to debug in IDE the '  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion , //"2.0.0",' section must be in comments
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion , //"2.0.0"
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion )

assemblyMergeStrategy in assembly := {
  case manifest if manifest.contains("MANIFEST.MF") =>
    // We don't need manifest files since sbt-assembly will create
    // one with the given settings
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    // Keep the content for all reference-overrides.conf files
    MergeStrategy.concat
  case deckfour if deckfour.contains("deckfour") || deckfour.contains(".cache")=>
    // For all the other files, use the default sbt-assembly merge strategy
    //val oldStrategy = (assemblyMergeStrategy in assembly).value
    //oldStrategy(x)
    //MergeStrategy.first
    MergeStrategy.last
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}
