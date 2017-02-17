package main.scala.mllib.features

/**
  * Created by hadoop on 2016/12/14.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object StopWrods {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StopWrods").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("StopWrods")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
    // clean up
    sc.stop()
  }
}
