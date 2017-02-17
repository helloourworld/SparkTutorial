/**
  * Created by david on 2016/11/5.
  */

import org.apache.spark.{SparkContext, SparkConf}
object Map {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)  //创建RDD
    val map = rdd.map(_*2)             //对RDD中的每个元素都乘于2
    println(args(0))
    map.foreach(x => println(x + " "))
    sc.stop()
  }
  }
