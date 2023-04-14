import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ListBuffer

object SensorStatic {

  case class SensorMeasurment(sensorid:String,humidity : String)
  var fileList: List[File] = null
  var mean = ""
  var humidityListBuffer = new ListBuffer[List[String]]()
  var sensoridListBuffer = new ListBuffer[List[String]]()
  var map: Map[String, ListBuffer[Int]] = Map()

  def numOfProcessedFiles(dir: String): Int = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      fileList = d.listFiles.filter(_.isFile).toList

    } else {
      List[File]()
    }

    return fileList.size
  }

  def numOfProcessedMeasurements1():Int={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf()
    config.set("spark.app.name","humiditySensorData")
    config.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(config).getOrCreate()

    val columnNames: List[String] = List("column1", "column2")

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

   val numOfProcessedMeasurements =  spark.sql("select count(*) as " +
     "numOfProcessedMeasurements from sensorTable ")
    numOfProcessedMeasurements.show()
    println("111111111111")
    val numOfFailedMeasurements = spark.sql("select count(*) as numOfFailedMeasurements  " +
      " from sensorTable where humidity = 'NaN'  ")
    numOfFailedMeasurements.show()
    println("2222222")
    val humidityData = spark.sql("select sensorid, min(cast(humidity as int)) as min, " +
      "cast(avg(cast(humidity as int))as int) as avg,max(cast(humidity as int)) as max" +
      " from sensorTable  group by sensorid  ")
    //humidityData.show()
    println("3333333333")
    val maxMinAvgHumidity = humidityData.withColumn("min",expr("coalesce(min,'NaN')"))
      .withColumn("avg",expr("coalesce(avg,'NaN')"))
      .withColumn("max",expr("coalesce(max,'NaN')"))
    maxMinAvgHumidity.show()

    maxMinAvgHumidity.createOrReplaceTempView("maxMinAvgHumidity")

    val highestAvgHumidity = spark.sql("select sensorid,max(cast(avg as int)) as highestAvg from maxMinAvgHumidity" +
      " group by sensorid order by highestAvg desc ").withColumn("highestAvg",expr("coalesce(highestAvg,'NaN')"))
    highestAvgHumidity.show()
    println("noooooooooo")
    //println("Num of processed files:" + totalSensorCount.show())

    spark.stop()
    1
  }

  def numOfProcessedMeasurements(): Int = {
    var conf = new SparkConf()
      .setAppName("Read CSV Files From Directory")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    for (file <- fileList) {
      val textRDD = sc.textFile(file.toString)
      val header = textRDD.first()
      val textRDDResult = textRDD.filter(row => row != header)
      val empRdd = textRDDResult.map {
        line =>
          val col = line.split(",")
          SensorMeasurment(col(0), col(1))
      }

      val humidityList = for {
        line <- empRdd
        values = line
        a = values.humidity
      } yield a

      humidityListBuffer += humidityList.collect().toList

      val sensoridList = for {
        line <- empRdd
        values = line
        a = values.sensorid
      } yield a

      sensoridListBuffer += sensoridList.collect().toList
    }
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    val finalHumidityList: List[Int] = humidityListBuffer.toList.flatten.map(e => if (e == "NaN") "0" else e).map(x => x.toInt)
    return finalSenserIDList.length
  }

  def numOfFailedMeasurements(): Int = {
    val HumidityListwithNaNData: List[String] = humidityListBuffer.toList.flatten
    var count = 0
    for (i <- 0 to (humidityListBuffer.toList.flatten.length - 1)) {
      if (HumidityListwithNaNData(i).equals("NaN")) {
        count = count + 1
      }
    }
    return count
  }

  def minAvgMaxHumidity(): Unit = {
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    val finalHumidityList: List[Int] = humidityListBuffer.toList.flatten.map(e => if (e == "NaN") "0" else e).map(x => x.toInt)
    var count1 = 0
    for (str <- finalSenserIDList) {
      if (map.contains(str)) {
        map += (str -> (map(str) += finalHumidityList(count1)))
      } else {
        map += (str -> ListBuffer(finalHumidityList(count1)))
      }
      count1 = count1 + 1
    }
    println("sensor-id" + "," + "min" + "," + "avg" + "," + "max :")
    for (i <- map) {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = (i._2.filterNot(p => p.equals(0)).sum)
      var avg = 0
      if (!remainder.isEmpty || !sum.equals(0)) {
        avg = (sum / remainder.size)
      } else {
        avg = 0
      }
      mean = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN" //(i._2.filterNot(p=> p.equals(0)).sum/(i._2.filterNot(p=> p.equals(0)).size))
      val min = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else i._2.filterNot(p => p.equals(0)).min
      val max = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else i._2.filterNot(p => p.equals(0)).max
      println(i._1 + "," + min + "," + mean + "," + max)
    }

  }

  def sortsSensorsByHighestAvgHumidity(): Unit = {
    var sortMap: Map[String, Int] = Map()
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    for (i <- map) {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = (i._2.filterNot(p => p.equals(0)).sum)
      var avg = 0
      if (!remainder.isEmpty || !sum.equals(0)) {
        avg = (sum / remainder.size)
      } else {
        avg = 0
      }
      mean = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN"
      val mean1 = if (i._2 == List(0)) "NaN" else mean
      if (mean1 != "NaN") {
        sortMap += (i._1 -> mean1.toInt)
      }
      else {
        sortMap += (i._1 -> 0)
      }
    }
    import scala.collection.immutable.ListMap
    print("sorts sensors by highest avg humidity :")
    for (i <- ListMap(sortMap.toSeq.sortWith(_._2 > _._2): _*)) {
      if (i._2 == 0)
        print(i._1 -> "NAN" + ", ");
      else
        print(i._1 -> i._2 + ", ");
    }
  }

}
