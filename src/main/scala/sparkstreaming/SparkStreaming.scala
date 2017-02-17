package main.scala.sparkstreaming

/**
  * Created by mac on 16/8/12.
  */
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object SparkStreaming {

  def main(args: Array[String]) {

    if (args.length != 1) {
      System.err.println("Usage: <watchDir>")
      System.exit(1)
    }

    //开本地线程两个处理，local[4]：意思本地起4个进程运行，setAppName("SparkStreaming")：设置运行处理类

    val conf = new SparkConf().setAppName("SparkStreaming")
    //每隔5秒计算一批数据local[4]：意思本地起4个进程运行，setAppName("SparkStreaming")：设置运行处理类
    val ssc = new StreamingContext(conf, Seconds(20))
    ssc.checkpoint("root")
    // 指定监控的目录
    val watchDir = args(0)
    val lines = ssc.textFileStream(watchDir)
    lines.checkpoint(Seconds(60))
    //按\t 切分输入数据
    val wordCounts = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    //val sortResult = wordCounts.transform(rdd => rdd.sortBy(_._2, false))
    wordCounts.print()
    ssc.start() // 开启计算
    ssc.awaitTermination() // 阻塞等待计算

  }

}


/*
从结果可以看出，
sparkstreaming每次会将设置的时间分片以内发生的增量日志进行一次批量处理，最终输出这个增量处理的结果。
*/