/**
  * Created by david on 2016/11/5.
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.SPARK_BRANCH
object Map {
  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("map")
//    val sc = new SparkContext(conf)
//    val rdd = sc.parallelize(1 to 10) //创建RDD
//    val map = rdd.map(_ * 2) //对RDD中的每个元素都乘于2
//    println(args(0))
//    map.foreach(x => println(x + " "))


    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicMap", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4))
    val result = input.map(x => x*x)
    println(System.getenv("SPARK_HOME"))
    println(org.apache.spark.SPARK_VERSION)
    result.foreach(x => println(x + " "))
  }
  }
//C:\spark-2.0.0-bin-hadoop2.7
//2.0.1
//1,4,9,16