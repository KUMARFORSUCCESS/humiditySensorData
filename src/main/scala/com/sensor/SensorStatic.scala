package com.sensor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File

object SensorStatic {

  var fileList: List[File] = null
  def getNumOfProcessedFiles(dir: String): Int = {
    val directory = new File(dir)
    if (directory.exists && directory.isDirectory) {
      fileList = directory.listFiles.filter(_.isFile).toList

    } else {
      List[File]()
    }
    return fileList.size
  }

  def getAllReports(){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf()
    config.set("spark.app.name","humiditySensorData")
    config.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(config).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // All dataframe columns are of type string
    val schema = StructType(List(
      StructField("sensorid", StringType),
      StructField("humidity", StringType),
    ))
    var allSensorData = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    for(file <- fileList){
      val filesDF =spark.read.format("csv")
        .option("header", true)
        .schema(schema)
        .option("path",file.toString)
        .load()
      allSensorData =  allSensorData.union(filesDF)
    }
    allSensorData.createOrReplaceTempView("sensorTable")
    //Num of processed measurements
   val numOfProcessedMeasurements =  spark.sql("select count(*) as " +
     "numOfProcessedMeasurements from sensorTable ")
    numOfProcessedMeasurements.show()

    //Num of failed measurements
    val numOfFailedMeasurements = spark.sql("select count(*) as numOfFailedMeasurements  " +
      " from sensorTable where humidity = 'NaN'  ")
    numOfFailedMeasurements.show()


    val humidityData = spark.sql("select sensorid, min(cast(humidity as int)) as min, " +
      "cast(avg(cast(humidity as int))as int) as avg,max(cast(humidity as int)) as max" +
      " from sensorTable  group by sensorid  ")

    //Report for to get min, max, avg of sensor's humidity
    println("Report for to get min, max, avg of sensor's humidity")
    val maxMinAvgHumidity = humidityData.withColumn("min",expr("coalesce(min,'NaN')"))
      .withColumn("avg",expr("coalesce(avg,'NaN')"))
      .withColumn("max",expr("coalesce(max,'NaN')"))
    maxMinAvgHumidity.show()

    //Sensors with highest avg humidity
    println("Sensors with highest avg humidity")
    maxMinAvgHumidity.createOrReplaceTempView("maxMinAvgHumidity")

    val highestAvgHumidity = spark.sql("select sensorid,max(cast(avg as int)) as highestAvg from maxMinAvgHumidity" +
      " group by sensorid order by highestAvg desc ").withColumn("highestAvg",expr("coalesce(highestAvg,'NaN')"))
    highestAvgHumidity.show()

    spark.stop()

  }


}
