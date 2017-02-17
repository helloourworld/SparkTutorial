package main.scala.mllib.stat

/**
  * Created by hadoop on 2016/11/17.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object Correlations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Correlation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series

    val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 55)) // must have the same number of partitions and cardinality as seriesX

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(s"Correlation is: $correlation")

    val data: RDD[Vector] = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(5.0, 33.0, 366.0))
    )  // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(correlMatrix.toString)
  }

}
