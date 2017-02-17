package main.scala.sparkstreaming

/*
原理是根据一个原始日志log，然后随机的从中挑选行添加到新生产的日志中，并且生产的数据量呈不断的增长态势
 */
import java.io._
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date
import java.io.PrintWriter
import main.scala.sparkbase.DeleteDir
import org.apache.hadoop.conf.Configuration

import scala.io.Source
import scala.util.matching.Regex
object FileGenerator {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Correlation").set("spark.executor.memory", "2g").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var i=0
    while (i<100 )
    {
      if (args.length != 2) {
        System.err.println("Usage: <filename> <watchDir>")
        System.exit(1)
      }

      val filename = args(0)
      val watchDir = args(1)
//      DeleteDir.delete(watchDir)

      val lines = Source.fromFile(filename).getLines.toList
      val filerow = lines.length

      val writer = new PrintWriter(new File(watchDir+i+".txt" ))
      i += 1
      var j=0
      while(j<i)
      {
        writer.write(lines(index(filerow)))
        println(lines(index(filerow)))
        j=j+1
      }
      writer.close()
      Thread sleep 5000
      log(getNowTime(),watchDir+i+".txt generated")
    }
  }
  def log(date: String, message: String)  = {
    println(date + "----" + message)
  }
  /**
    * 从每行日志解析出imei和logid
    *
    **/
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }
  def getNowTime():String={
    val now:Date = new Date()
    val datetimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val ntime = datetimeFormat.format( now )
    ntime
  }
  /**
    * 根据时间字符串获取时间,单位(秒)
    *
    **/
  def getTimeByString(timeString: String): Long = {
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sf.parse(timeString).getTime / 1000
  }
}