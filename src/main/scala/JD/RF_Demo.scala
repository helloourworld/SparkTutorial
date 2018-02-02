package main.scala.JD

/**
  * Created by yulijun on 2018/1/26.
  */

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object RF_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RF-Demo").setMaster("local[2]")
    conf.set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/rf/rf (2).libsvm")
    // Split the data into training and test sets(30% held out for testing)
    val splits = data.randomSplit(Array(0.6, 0.4))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]((1, 2)
    )
    val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algo choose
    val impurity = "gini"
    val maxDepth = 8
    val maxBins = 2

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map{point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)
    // Instantiate metrics object

    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    // Confusion matrix
    println("Confusion matrix:")
    val metrics2 = new MulticlassMetrics(labelAndPreds)
    println(metrics2.confusionMatrix)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }
    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)


//    // Save and load model
//     model.save(sc, "RandomForestClassificationModel")
//     val sameModel = RandomForestModel.load(sc, "RandomForestClassificationModel")

    // clean up
    sc.stop()
  }
}