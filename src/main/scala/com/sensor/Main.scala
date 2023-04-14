package com.sensor

import org.apache.log4j.{Level, Logger}

object Main extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  var dir = "data/"

  val getNumOfProcessedFiles = SensorStatic.getNumOfProcessedFiles(dir)
  println("Number of files processed :" + getNumOfProcessedFiles)
  //getAllReports() will give all reports excepts number of processed files
  SensorStatic.getAllReports()

  }