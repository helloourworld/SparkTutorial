package rddtransform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 10.LeftOutJoin(otherDataSet，numPartitions):左外连接，包含左RDD的所有数据，
  * 如果右边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度
  */
object LeftOutJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("leftOutJoin")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3),("C",1))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val leftOutJoinRDD = rdd.leftOuterJoin(rdd1)
    leftOutJoinRDD .foreach(println)
    sc.stop
  }
}
