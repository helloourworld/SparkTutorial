package main.scala.mllib.features

/**
  * Created by hadoop on 2014/12/14.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object TF_IDF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TF_IDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
    val spark = SparkSession
      .builder
      .appName("GroupBy Test")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"),
      (3, "spark java")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(10).foreach(println)
    // clean up
    sc.stop()
  }
}
