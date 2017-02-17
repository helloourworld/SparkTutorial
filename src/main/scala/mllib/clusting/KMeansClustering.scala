package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/15.
  * data/mllib/UCI/Wholesale_customers_data.csv 8 30 3
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansClustering {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage:KMeansClustering rawDataFilePath numClusters numIterations runTimes")
      sys.exit(1)
    }
    val conf = new SparkConf().setAppName("KMeansClustering").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * Channel Region Fresh   Milk Grocery Frozen Detergents_Paper Delicassen
      * 2       3    12669 9656 7561   214     2674                1338
      * 2       3    7057 9810 9568    1762    3293                1776
      * 2       3    6353 8808 7684    2405    3516                7844
      */

    val rawTrainingData = sc.textFile(args(0))
    val rawData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {

        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).randomSplit(Array(0.7, 0.3))

    // Cluster the data into two classes using KMeans
    val parsedTrainingData = rawData(0).cache()

    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val runTimes = args(3).toInt
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":" + x)
        clusterIndex += 1
      })

    //begin to check which cluster each test data belongs to based on the clustering result
    val parsedTestData = rawData(1)
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")
  }

  private def
  isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
