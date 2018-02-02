package main.scala.JD

/**
  * Created by yulijun on 2017/12/11.
  */

  import org.apache.spark.mllib.feature.StandardScaler
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.{SparkConf, SparkContext}

object StandardScaler {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StandardScaler").setMaster("local")
    conf.set("spark.testing.memory", "5147480000")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "C:\\spark-2.0.2-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")

    //1.不设置任何参数，自动是默认的withMean=False and withStd=True
    //注：new StandardScaler().只有一个fit的方法，fit里可传入一个Vector 或 RDD[Vector]
    //使用fit生成的是一个StandardScalerModel
    val scaler1 = new StandardScaler().fit(data.map(x => x.features))

    //StandardScalerModel的变量与方法
    //首先可以输出以下变量：模型的均值（Vector),标准差（Vector),是否withMean（Boolean),是否withStd（Boolean)
    val mean = scaler1.mean
    val std = scaler1.std
    val isMean = scaler1.withMean
    val isStd = scaler1.withStd

    //其次可以调用三类方法：设置withMean参数，设置withStd参数，transform转化成规模化后的RDD[Vector]
    //注：调用transform,可输入RDD[Vector],或Vector
    val setMean = scaler1.setWithMean(false)
    val setStd = scaler1.setWithStd(true)
    val transformData = data.map(x => (x.label, scaler1.transform(x.features)))


    //2.在建立模型的时候设置参数，以下表示方差为1，均值为0
    val scaler2 = new StandardScaler(withMean = true, withStd = true)
      .fit(data.map(x => x.features))

    //使用transform方法输出规模化后的数据
    //要注意的是，当withMean设置的是true时，输入的Vector必须是dense的！
    val transformData2 =data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))

  }
}
