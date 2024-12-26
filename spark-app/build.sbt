name := "crypto-analytics"
version := "0.1"
scalaVersion := "2.12.15"

val sparkVersion = "3.1.2"  // Compatible with JDK 11

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1"
)

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

scalacOptions ++= Seq(
  "-Ywarn-unused",
  "-Ywarn-unused-import"
)