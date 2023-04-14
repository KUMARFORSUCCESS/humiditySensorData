import SensorProvidedData.FileDetails
import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}

import java.util.Properties
import scala.collection.JavaConverters._

object Main extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  /*val props: Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("topic_text")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      var recordsVector = records.iterator().asScala.toVector
      for (record  <- recordsVector) {
  println(record)
        val gson = new Gson
        //getGsonObject().fromJson(message, KafkaEventData.class);
        val jsonString: String =
          """
               {"name":"Dheeraj","address":[{"city":"Ghaziabad","state":"UP"},{"city":"Delhi","state":"Delhi"}]}
             """
        val fileDetails:FileDetails = gson.fromJson(jsonString,classOf[FileDetails])
          //fromJson(record.value(),classOf[FileDetails])
      }
    }
    }
    finally
    {
      consumer.close()
    }*/

  var dir = "Data/"

//  val sensorSpark = new SensorStatic()
  val noOfProcessedFile = SensorStatic.numOfProcessedFiles(dir)
  println("Num of processed files:" + noOfProcessedFile)
  SensorStatic.numOfProcessedMeasurements1()
  /*val numOfProcessedMeasure = SensorStatic.numOfProcessedMeasurements()
  println("Num of processed measurements: " + numOfProcessedMeasure)
  val numOfFailedMeasure = SensorStatic.numOfFailedMeasurements()
  println("Num of failed measurements: " + numOfFailedMeasure)
  SensorStatic.minAvgMaxHumidity()
  SensorStatic.sortsSensorsByHighestAvgHumidity()*/
  }