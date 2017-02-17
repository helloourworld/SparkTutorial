package rddtransform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 9.join(otherDataSet,numPartitions):对两个RDD先进行cogroup操作形成新的RDD，
  * 再对每个Key下的元素进行笛卡尔积，numPartitions设置分区数，提高作业并行度
  */
object Join {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("join")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd.join(rdd1)
    groupByKeyRDD.foreach(println)
  }
}
