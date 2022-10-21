ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_processing"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.2.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark--connector" % "3.3.0"
