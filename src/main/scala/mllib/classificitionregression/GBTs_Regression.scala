package main.scala.mllib.classificitionregression

/**
  * Created by hadoop on 2017/4/6.
  */

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object GBTs_Regression {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GBTs Regression").setMaster("local[2]")
    val sc = new SparkContext(conf)
  // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.

    boostingStrategy.treeStrategy.maxDepth = 5
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression GBT model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
//    val sameModel = GradientBoostedTreesModel.load(sc,
//      "target/tmp/myGradientBoostingClassificationModel")
    // clean up
    sc.stop()
  }
}
