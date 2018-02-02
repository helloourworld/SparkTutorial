package main.scala.JD

/**
  * Created by yulijun on 2018/1/26.
  */

import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkConf, SparkContext}

object RF_Score {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RF-Demo").setMaster("local[2]")
    conf.set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // load model
    val sameModel = RandomForestModel.load(sc, "RandomForestClassificationModel")
    println(sameModel)


    // load data and score



    // clean up
    sc.stop()
  }
}