package rddtransform

import org.apache.spark._

/**
  * 7.sortByKey(accending，numPartitions):返回以Key排序的（K,V）键值对组成的RDD，accending为true时表示升序，
  * 为false时表示降序，numPartitions设置分区数，提高作业并行度
  */
object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortByKey")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr)
    val sortByKeyRDD = rdd.sortByKey(true)
    sortByKeyRDD.foreach(println)
    sc.stop
  }
}
