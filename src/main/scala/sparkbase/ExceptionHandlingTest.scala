package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/1.
  */


import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExceptionHandlingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExceptionHandling").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .appName("ExceptionHandlingTest")
      .getOrCreate()
    println(spark.sparkContext.defaultParallelism)
    spark.sparkContext.parallelize(0 until spark.sparkContext.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }

    spark.stop()
  }
}
