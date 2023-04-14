ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"
showSuccess := false

libraryDependencies +="org.apache.kafka" % "kafka-clients" % "3.4.0"
// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "1.7.1"

/*libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.5.6" % Test*/
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies +=  "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies +="org.apache.spark" %% "spark-sql" % "3.3.0"

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
lazy val root = (project in file("."))
  .settings(
    name := "humiditySensorData"

  )
