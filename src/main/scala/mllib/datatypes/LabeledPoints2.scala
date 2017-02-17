package main.scala.mllib.datatypes

/**
  * Created by hadoop on 2016/11/17.
  */
//label index1:value1 index2:value2 ...

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object LabeledPoints2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LabeldPoint").setMaster("local[2]")
    val sc = new SparkContext(conf)

    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.rdd.RDD

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    examples.collect()
  }

}
