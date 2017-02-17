package rddtransform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 11.RightOutJoin(otherDataSet, numPartitions):右外连接，包含右RDD的所有数据，
  * 如果左边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度
  */
//spark://NN01.HadoopVM:7077
object RightOutJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rightOutJoin").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C","C1"))
    val rdd = sc.parallelize(arr, 2)
    val rdd1 = sc.parallelize(arr1, 2)
    val rightOutJoinRDD = rdd.rightOuterJoin(rdd1)
    rightOutJoinRDD.foreach(println)
    sc.stop
  }
}
