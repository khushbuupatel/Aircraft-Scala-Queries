name := "WJ"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.2"
)



