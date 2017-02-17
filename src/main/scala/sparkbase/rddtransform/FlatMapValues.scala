package rddtransform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2 flatMapValues
  */
object FlatMapValues {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list,2)
    val mapValuesRDD = rdd.flatMapValues(x => Seq("male",x))
    mapValuesRDD.foreach(println)
  }
}

//(mobin,22)
//(mobin,male)
//(kpop,20)
//(kpop,male)
//(lufei,23)
//(lufei,male)

//(mobin,List(22, male))
//(kpop,List(20, male))
//(lufei,List(23, male))
