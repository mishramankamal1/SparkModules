name := "SparkModules"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion="3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
