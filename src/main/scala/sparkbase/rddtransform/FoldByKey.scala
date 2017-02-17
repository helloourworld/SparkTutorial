package rddtransform

import org.apache.spark._
/**
  * 4 foldByKey函数是通过调用CombineByKey函数实现的
  * foldByKey(zeroValue,partitioner)(func)
  * foldByKey(zeroValue,numPartitiones)(func)
  * zeroVale：对V进行初始化，实际上是通过CombineByKey的createCombiner实现的  V =>  (zeroValue,V)，再通过func函数映射成新的值，即func(zeroValue,V),如例4可看作对每个V先进行  V=> 2 + V
  * func: Value将通过func函数按Key值进行合并（实际上是通过CombineByKey的mergeValue，mergeCombiners函数实现的，只不过在这里，这两个函数是相同的）
  */
object FoldByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    val foldByKeyRDD = {
      rdd.foldByKey(0)(_ + _)
    }
    foldByKeyRDD.foreach(println)
  }
}