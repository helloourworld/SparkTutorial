package main.scala.mllib.stat

/**
  * Created by hadoop on 2016/11/17.
  */

import org.apache.spark.{SparkConf, SparkContext}

object HypothesisTesting2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Correlation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD

    val data: RDD[Double] = sc.parallelize(Seq(0.1, 0.15, 0.2, 0.3, 0.25))  // an RDD of sample data

    // run a KS test for the sample versus a standard normal distribution
    val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
    // summary of the test including the p-value, test statistic, and null hypothesis if our p-value
    // indicates significance, we can reject the null hypothesis.
    println(testResult)
    println()

    // perform a KS test using a cumulative distribution function of our making
    val myCDF = Map(0.1 -> 0.2, 0.15 -> 0.6, 0.2 -> 0.05, 0.3 -> 0.05, 0.25 -> 0.1)
    val testResult2 = Statistics.kolmogorovSmirnovTest(data, myCDF)
    println(testResult2)

  }

}
