package main

/**
  * Created by mac on 16/8/12.
  */

import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.util.AccumulatorV2;
object SparkConnectTest {

  def main(args: Array[String]) {
    //开本地线程两个处理，local[4]：意思本地起4个进程运行，setAppName("SparkStreaming")：设置运行处理类

    val conf = new SparkConf().setAppName("SparkConnect").setMaster("local[2]")

    val sc = new SparkContext(conf)



    val lines = sc.parallelize(Seq("hello world", "hello tencent"))
    val wc = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.foreach(println)


    Thread.sleep(3 * 60 * 1000) // 挂住 2 分钟; 这时可以去看 SparkUI: http://localhost:4040


//    val wordCounts = sc.textFile("README.md").flatMap(lines => lines.split(" " )).map(x => (x,1)).reduceByKey(_+_)
//
//    val sortResult = wordCounts.sortBy(_._2,true)
//
//    sortResult.foreach(println)
//
//    sortResult.foreach(println)
  }
}

/*
从结果可以看出，
sparkstreaming每次会将设置的时间分片以内发生的增量日志进行一次批量处理，最终输出这个增量处理的结果。
*/