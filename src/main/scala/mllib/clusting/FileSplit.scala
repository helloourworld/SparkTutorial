package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/14.
  */

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object FileSplit {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("FileSplit").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rawTrainingData = sc.textFile("data/mllib/UCI/Wholesale_customers_data.csv")

    val rawData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {

        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).randomSplit(Array(0.7, 0.3))

    val parsedTrainingData = rawData(0)
    val parsedTestData = rawData(1)
    println(rawData.size)
    println(rawTrainingData.count())
    println(parsedTrainingData.count())
    println(parsedTestData.count())
    parsedTrainingData.take(10).foreach(println)

  }

  private def
  isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
