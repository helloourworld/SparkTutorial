package main.scala.mllib.classificitionregression

/**
  * Created by hadoop on 2016/12/5.
  */

import org.apache.spark.{SparkConf, SparkContext}


object NB {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NB").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
    import org.apache.spark.mllib.util.MLUtils

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // clean up
    sc.stop()
  }
}
