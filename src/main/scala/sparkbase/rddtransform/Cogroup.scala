package rddtransform

import org.apache.spark._

/**
  * 8.cogroup(otherDataSet，numPartitions)：对两个RDD(如:(K,V)和(K,W))相同Key的元素先分别做聚合，
  * 最后返回(K,Iterator<V>,Iterator<W>)形式的RDD,numPartitions设置分区数，提高作业并行度
  */
object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cogroup")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd1 = sc.parallelize(arr, 3)
    val rdd2 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd1.cogroup(rdd2)
    groupByKeyRDD.foreach(println)
    sc.stop
    val sourceURL = "jdbc:postgresql://171.0.11.4:25108/poc"
    val username = "omm"
    val password = "Gaussdba@Mpp"

    val url = sourceURL.concat("?user=").concat(username).concat("&password=").concat(password)
    println(url)
  }
}
