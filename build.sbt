ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies +="org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test
lazy val root = (project in file("."))
  .settings(
    name := "humiditySensorData"

  )
