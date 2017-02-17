package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/1.
  */

import org.apache.spark.{SparkConf, SparkContext}
import java.util.Random

import org.apache.spark.sql.SparkSession
object GroupByTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkExp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .appName("GroupBy Test")
      .getOrCreate()

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers

    val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())

    spark.stop()
    // clean up
    sc.stop()
  }
}
