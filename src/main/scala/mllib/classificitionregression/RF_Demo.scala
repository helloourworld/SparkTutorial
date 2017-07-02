package main.scala.mllib.classificitionregression

/**
  * Created by hadoop on 2017/4/6.
  * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-random-forest/
  * http://stackoverflow.com/questions/25038294/how-do-i-run-the-spark-decision-tree-with-a-categorical-feature-set-using-scala
  */

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RF_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RF-Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/rf/rf.libsvm")
    // Split the data into training and test sets(30% held out for testing)
    val splits = data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]((0,2),(1,2))
    val numTrees = 1 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algo choose
    val impurity = "gini"
    val maxDepth = 3
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
