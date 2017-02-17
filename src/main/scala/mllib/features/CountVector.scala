package main.scala.mllib.features

/**
  * Created by hadoop on 2016/12/14.
  */

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CountVector {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CountVector").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
    val spark = SparkSession
      .builder
      .appName("CounterVector")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).select("features").collect().foreach(println)
    // clean up
    sc.stop()
  }
}
