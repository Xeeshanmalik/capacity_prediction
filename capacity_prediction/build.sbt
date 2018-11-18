name := "timeseries"

version := "0.1"

scalaVersion := "2.11.1"


libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"


libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"


libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"


//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"