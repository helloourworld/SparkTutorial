package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/14.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object KMeansDF {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("KMeansDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("KMeansDF")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    val sourceUrl = "jdbc:mysql://192.168.71.128:3306/test?user=hive&password=hive"
    val df = spark
      .read
      .format("jdbc")
      .option("url", sourceUrl)
      .option("dbtable", "wholesales")
      .load()
    //    val rawTrainingData = sc.textFile("data/mllib/UCI/Wholesale_customers_data.csv")

    val rawData =
      df.randomSplit(Array(0.7, 0.3))

    val parsedTrainingData = rawData(0)
    val parsedTestData = rawData(1)
    println("split to: " + rawData.size)
    println("rawDataSize: " + df.count())
    println("parsedTrainingData " + parsedTrainingData.count())
    println("parsedTestData " + parsedTestData.count())
    parsedTrainingData.take(10).foreach(println)

  }
}
