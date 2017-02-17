package main.scala.sparkstreaming

/**
  * Created by root on 16-11-1.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WindowWordCount {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <filename> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")

    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    //val words = lines.flatMap(_.split(","))
    val words = lines.map(_.split(",")).filter(_.length == 6).filter(x => x(5).toDouble > 0.0 )
    //第二个参数是 windows的窗口时间间隔，比如是 监听间隔的 倍数，上面是 5秒，这里必须是5的倍数。eg :30
    //第三个参数是 windows的滑动时间间隔，也必须是监听间隔的倍数。eg :10
    //那么这里的作用是， 每隔10秒钟，对前30秒的数据， 进行一次处理，这里的处理就是 word count。


    // windows操作，第一种方式为叠加处理，第二种方式为增量处理
    //val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))

    //这个是优化方法， 即加上上一次的结果，减去 上一次存在又不在这一次的数据块的部分。
    //val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow(_+_, _-_,Seconds(args(2).toInt), Seconds(args(3).toInt))

    //val wordCounts = words.map(x => (x(2),x(5).toDouble)).reduceByKeyAndWindow((a:Double,b:Double) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))

    val maxWord = words.map(x => (x(0),x(5).toDouble)).reduceByKeyAndWindow((a:Double,b:Double) => if (a > b) a else b, Seconds(args(2).toInt), Seconds(args(3).toInt))
    val minWord = words.map(x => (x(0),x(5).toDouble)).reduceByKeyAndWindow((a:Double,b:Double) => if (a > b) b else a, Seconds(args(2).toInt), Seconds(args(3).toInt))
    val join = maxWord.fullOuterJoin(minWord)
    join.map{x =>
            val max:Double = x._2._1.getOrElse(0)
            val min:Double = x._2._2.getOrElse(0)
            val res = (max-min)/min
            (x._1, res)
    }.filter(x => x._2 > 0.3).print()
    ssc.start()
    ssc.awaitTermination()
  }
}