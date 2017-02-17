package rddtransform

import org.apache.spark._

/**
  * 6.groupByKey(numPartitions):按Key进行分组，返回[K,Iterable[V]]，numPartitions设置分区数，提高作业并行度
  */
object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr)
    val groupByKeyRDD = rdd.groupByKey()
    groupByKeyRDD.foreach(println)
    sc.stop
  }
}
