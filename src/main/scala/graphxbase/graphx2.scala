package main.scala.graphxbase

/**
  * Created by hadoop on 2017/1/9.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object graphx2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graphx1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("graphx1")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()

    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    val vertexArray = Array(
      (1L, ("A", 30)),
      (2L, ("B", 23)),
      (6L, ("C", 99))
    )
    val edgeArray = Array(
      Edge(1L, 2L, 9),
      Edge(1L, 6L, 3)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph = Graph(vertexRDD, edgeRDD)
    // Solution 1
    graph.vertices.filter {
      case (id, (name, age)) => age > 20
    }.collect.foreach {
      println
    }
    for (triplet <- graph.triplets.collect) {
      /**
        * Triplet has the following Fields:
        * triplet.srcAttr: (String, Int) // triplet.srcAttr._1 is the name
        * triplet.dstAttr: (String, Int)
        * triplet.attr: Int
        * triplet.srcId: VertexId
        * triplet.dstId: VertexId
        */
      println(triplet)
    }
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect){
      println(s"${triplet.srcAttr._1} who is ${triplet.srcAttr._2} loves ${triplet.dstAttr._1} who is ${triplet.dstAttr._2}")
    }

  }
}
