package main.scala.mllib.clusting

/**
  * Created by hadoop on 2017/4/7.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{GaussianMixtureModel, GaussianMixture}
import org.apache.spark.mllib.linalg.Vectors

object Gaussianmixture {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Gaussianmixture").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Gaussianmixture")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    // Load and parse the data
    val data = sc.textFile("data/mllib/gmm_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using GaussianMixture
    val gmm = new GaussianMixture().setK(2).run(parsedData)

    // Save and load model
//    gmm.save(sc, "/target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
//    val sameModel = GaussianMixtureModel.load(sc,
//      "/target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")

    // output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }

    // clean up
    sc.stop()
  }
}
