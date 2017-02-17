package main.scala.graphxbase

/**
  * Created by hadoop on 2017/1/9.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object graphx1 {
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

    // Solution 1
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    // Solution 2
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    // Solution 3
    for ((id,(name,age)) <- graph.vertices.filter { case (id,(name,age)) => age > 30 }.collect) {
      println(s"$name is $age")
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
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} who is ${triplet.srcAttr._2} loves ${triplet.dstAttr._1} who is ${triplet.dstAttr._2}")
    }
  }
}
