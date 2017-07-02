package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/15.
  * http://blog.csdn.net/guohecang/article/details/51307935
  */

import org.apache.spark.{SparkConf, SparkContext}

object RDDRandomSplit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDRandomSplit").setMaster("local[2]")
    val sc = new SparkContext(conf)

    var rdd = sc.makeRDD(1 to 1000, 10)
    rdd.collect()

    var splitRDD = rdd.randomSplit(Array(0.7, 0.3))

    println(splitRDD.size)

    splitRDD(0).collect()
    println("0: " + "%1.1f".format(splitRDD(0).count() / rdd.count().toFloat))

    splitRDD(1).collect()
    println("1: " + "%1.1f".format(splitRDD(1).count() / rdd.count().toFloat))


    // clean up
    sc.stop()
  }
}
