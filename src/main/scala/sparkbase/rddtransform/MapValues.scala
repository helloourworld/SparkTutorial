package rddtransform

import org.apache.spark.{SparkConf, SparkContext}
/**
  * 1 mapValues
  */
object MapValues {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)
    val mapValuesRDD = rdd.mapValues(_+2)
    mapValuesRDD.foreach(println)
  }
}
