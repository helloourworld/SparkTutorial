package main.scala.sparkstreaming

/**
  * Created by hadoop on 2016/12/12.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Kafka010Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafka010Test").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(20))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "NN01.HadoopVM:9092,DN01.HadoopVM:9092,DN02.HadoopVM:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "movieLens",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("movie")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(rdd => {
      rdd.foreach(x => {
        println(x.value())
      })
    })
    // clean up
    ssc.start()
    ssc.awaitTermination()
  }
}
