package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/15.
  */

import org.apache.spark.{SparkConf, SparkContext}


object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 9, 3)
//    查看lieanage
//    println(a.toDebugString)
    a.mapPartitions(myfunc).collect().foreach(println)
    // clean up
    sc.stop()
  }

  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    //    函数myfunc是把分区中一个元素和它的下一个元素组成一个Tuple。因为分区中最后一个元素没有下一个元素了，所以(3,4)和(6,7)不在结果中。
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext){
      val cur = iter.next
      res   .::= (pre,cur)
      pre = cur
    }
    res.iterator
  }
}
