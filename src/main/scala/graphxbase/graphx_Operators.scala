package main.scala.graphxbase

/**
  * Created by hadoop on 2017/1/9.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object graphx_Operators {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graphx_Operators").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //  Now we are ready to build a property graph. The basic property graph constructor takes an RDD of vertices
    // (with type RDD[(VertexId, V)]) and an RDD of edges (with type RDD[Edge[E]]) and builds a graph (with type
    // Graph[V, E]).
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
  }
}
