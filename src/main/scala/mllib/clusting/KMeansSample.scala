package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/15.
  */

import main.scala.sparkbase.DelHDFS
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 该程序主要通过kmeans算法对数据进行分类
  *执行方式：./spark-submit --master=spark://cloud25:7077 --class com.eric.spark.mllib.KMeansSample --executor-memory=2g /opt/cloud/spark-1.4.1-bin-hadoop2.6/lib/spark_scala.jar
  * Created by Eric on 2015/11/12.
  */
object KMeansSample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置环境
    val sparkConconf=new SparkConf().setAppName("KMeansSample").setMaster("local[*]")
    val sc=new SparkContext(sparkConconf)

    //装载数据
    val fileData=sc.textFile("data/mllib/kmeans_data.txt",1)
    val parseData=fileData.map(record=>Vectors.dense(record.split(" ").map(_.toDouble))).cache()

    //模型训练
    val k=2;
    val maxIterations=20
    val runs = 2
    val initializationMode ="k-means||"
    val model=KMeans.train(parseData,k,maxIterations,runs, initializationMode)

    //数据模型的中心点
    //    运行结果:
    //      Cluster centers:
    //    [0.1,0.1,0.1]
    //    [9.1,9.1,9.1]
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    //使用模型测试单点数据
    //运行结果
    //    Vectors 0.2 0.2 0.2 is belongs to clusters:0
    //    Vectors 0.25 0.25 0.25 is belongs to clusters:0
    //    Vectors 8 8 8 is belongs to clusters:1
    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
    println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))


    //交叉评估1，只返回结果
    val testdata = fileData.map(s =>Vectors.dense(s.split(' ').map(_.toDouble)))
    val result1 = model.predict(testdata)
    DelHDFS.delete("hdfs://NN01.HadoopVM:9000", "/data/mllib/result1")
    result1.saveAsTextFile("/data/mllib/result1")

    //交叉评估2，返回数据集和结果
    DelHDFS.delete("hdfs://NN01.HadoopVM:9000",  "/data/mllib/result2")
    val result2 = fileData.map {
      line =>
        val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
        val prediction = model.predict(linevectore)
        line + " " + prediction
    }.saveAsTextFile("/data/mllib/result2")

    //3计算测试误差Evaluate clustering by computing Within Set Sum of Squared Errors
    val cost =model.computeCost(testdata)
    println(s"Total cost = $cost.")
    sc.stop()


  }
}