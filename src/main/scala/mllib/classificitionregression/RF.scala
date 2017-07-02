package main.scala.mllib.classificitionregression

/**
  * Created by hadoop on 2017/4/6.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

object RF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("RF")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets(30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algo choose
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map{point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myRandomForestClassificationModel")
//    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

    // clean up
    sc.stop()
  }
}
