val sparkVersion = "3.0.0"

ThisBuild / name := "Siesta"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "auth.datalab"
ThisBuild / Test / parallelExecution := false

assembly / test := {}
assembly / mainClass := Some("auth.datalab.siesta.Main")
scalacOptions += "-deprecation"
javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
//scala
//    libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.6",
//    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1",
libraryDependencies += "de.uni.freiburg.iig.telematik" % "SEWOL" % "1.0.2" //read data
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0" //parser for the commandlines

libraryDependencies += "com.github.jnr" % "jnr-posix" % "3.1.15"
libraryDependencies += "joda-time" % "joda-time" % "2.12.2"

//to run the sbt assembly the '% "provided",' section must not be in comments
//to debug in IDE the '  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion , //"2.0.0",' section must be in comments
libraryDependencies ++= Seq(
  //      "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion, //"2.0.0"
  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
  //      "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion)


//
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.14.0"
  )
}



//minio (put in comments if want to execute cassandra
//libraryDependencies += "io.minio" %% "spark-select" % "2.1"
//libraryDependencies += "io.minio" % "minio" % "3.0.12"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.0" //3.0.3
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0" //was 2.4.2
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-assembly" % "3.2.0"

assembly / assemblyMergeStrategy:= {
  case manifest if manifest.contains("MANIFEST.MF") =>
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    MergeStrategy.concat
  case deckfour if deckfour.contains("deckfour") || deckfour.contains(".cache") =>
    MergeStrategy.last
  case x =>
    MergeStrategy.last
}

//lazy val root = (project in file("."))
//  .settings(
//    //test
//
//  )


//lazy val cassandra = (project in file("."))
//  .dependsOn(root)
//  .settings(
//    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"
//  )
//
//lazy val s3 = (project in file("."))
//  .dependsOn(root)
//  .settings(
//    //minio
//    libraryDependencies += "io.minio" % "spark-select_2.11" % "2.1",
//    libraryDependencies += "io.minio" % "minio" % "3.0.12",
//    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.0.3",
//    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.0.3",
//    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.0.3"
//  )
//