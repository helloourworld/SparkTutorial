package main.scala.mllib.stat

/**
  * Created by hadoop on 2016/11/17.
  */


import org.apache.spark.{SparkConf, SparkContext}

object DataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Correlation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.mllib.random.RandomRDDs._
    // Generate a random double RDD that contains 1 million i.i.d. values drawn from the
    // standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
    val u = normalRDD(sc, 1000000L, 10)
    // Apply a transform to get a random double RDD following `N(1, 4)`.
    val v = u.map(x => 1.0 + 2.0 * x)

    println(v.count())

    println(v.mean().round)

    println(v.variance().round)

  }
}
