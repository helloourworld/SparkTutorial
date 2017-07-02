package main.scala.Exams

/**
  * Created by hadoop on 2017/3/1.
  *
  */

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("secondarysort.txt")
    val rdd = file.map(line => line.split(" ")).
      map(x => (x(0), x(1))).groupByKey().
      sortByKey(true)
      .map(x => (x._1, x._2.toList.sortWith(_ > _)))
    rdd.collect.foreach(println)
    println("*"*50)
    val rdd2 = rdd.flatMap {
      x =>
        val len = x._2.length
        val array = new Array[(String, String)](len)
        for (i <- 0 until len) {
          array(i) = (x._1, x._2(i))
        }
        array
    }
    rdd2.collect.foreach(println)
    // clean up
    sc.stop()
  }
}
