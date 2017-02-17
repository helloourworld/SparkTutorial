package main.scala.sparkbase

/**
  * Created by hadoop on 2016/11/23.
  */

import org.apache.spark.{SparkConf, SparkContext}

object DelHDFSTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Correlation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    DelHDFS.delete("hdfs://NN01.HadoopVM:9000","/user/hadoop/target/tmp/myCollaborativeFilter/metadata")
  }
}
