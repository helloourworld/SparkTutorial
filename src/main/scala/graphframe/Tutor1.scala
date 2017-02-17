package graphframe

/**
  * Created by hadoop on 2016/11/29.
  */
import scala.collection.JavaConversions
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph._
import org.graphstream.graph.implementations.SingleGraph

object Tutor1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Correlation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val graph = new SingleGraph("Tutorial1")

    graph.setStrict(false)
    graph.setAutoCreate(true)
    graph.addEdge("AB", "A", "B")
    graph.addEdge("BC", "B", "C")
    graph.addEdge("CA", "C", "A")
    graph.display()

    // clean up
    sc.stop()
  }
}
