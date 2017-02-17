/**
  * Created by david on 2016/11/6.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage:<File>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    val words = line.flatMap(_.split(" ")).map((_, 1))
    val reducewords = words.reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }

}