package main.scala.mllib.classificitionregression

/**
  * Created by hadoop on 2016/12/6.
  */

import org.apache.spark.{SparkConf, SparkContext}

object DT {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DT").setMaster("local[2]")
    val sc = new SparkContext(conf)
    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.model.DecisionTreeModel
    import org.apache.spark.mllib.util.MLUtils
    // Load and parse the data file.
//    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    val data = MLUtils.loadLibSVMFile(sc, "data/rf/rf.libsvm")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]((0,2),(1,2))
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    // $example off$
    // clean up
    sc.stop()
  }
}

//如果网站新注册了一个用户，他在城市无房产、年收入小于 35w 且离过婚，则可以预测女孩不会跟他见面。通过上面这个简单的例子可以看出，决策树对于现实生活具有很强的指导意义。通过该例子，我们也可以总结出决策树的构建步骤：
//将所有记录看作是一个节点
//遍历每个变量的每种分割方式，找到最好的分割点
//利用分割点将记录分割成两个子结点 C1 和 C2
//对子结点 C1 和 C2 重复执行步骤 2）、3），直到满足特定条件为止
//
//If (feature 0 in {0.0})
//If (feature 1 in {0.0})
//Predict: 1.0
//Else (feature 1 not in {0.0})
//If (feature 2 <= 17.0)
//Predict: 0.0
//Else (feature 2 > 17.0)
//Predict: 1.0
//Else (feature 0 not in {0.0})
//Predict: 1.0