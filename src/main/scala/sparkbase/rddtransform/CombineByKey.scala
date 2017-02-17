package rddtransform

import org.apache.spark._
/**
  * 3 combineByKey
  *   统计男性和女生的个数，并以（性别，（名字，名字....），个数）的形式输出
  *
  *   comineByKey(createCombiner,mergeValue,mergeCombiners,partitioner,mapSideCombine)
  *   comineByKey(createCombiner,mergeValue,mergeCombiners,numPartitions)
  *   comineByKey(createCombiner,mergeValue,mergeCombiners)
  *   createCombiner:在第一次遇到Key时创建组合器函数，将RDD数据集中的V类型值转换C类型值（V => C）
  *   mergeValue：合并值函数，再次遇到相同的Key时，将createCombiner道理的C类型值与这次传入的V类型值合并成一个C类型值（C,V）=>C
  *   mergeCombiners:合并组合器函数，将C类型值两两合并成一个C类型值
  *   partitioner：使用已有的或自定义的分区函数，默认是HashPartitioner
  *   mapSideCombine：是否在map端进行Combine操作,默认为true
  */
object CombineByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("combinByKey")
    val sc = new SparkContext(conf)
    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
    val rdd = sc.parallelize(people)
    val combinByKeyRDD = rdd.combineByKey(
      (x: String) => (List(x), 1),
      (peo: (List[String], Int), x : String) => (x :: peo._1, peo._2 + 1),
      (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2))

    combinByKeyRDD.foreach(println)
    sc.stop()
  }
}
