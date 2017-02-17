package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/15.
  */

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansClusteringKChoiced {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansClusteringKChoiced").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * Channel Region Fresh   Milk Grocery Frozen Detergents_Paper Delicassen
      * 2       3    12669 9656 7561   214     2674                1338
      * 2       3    7057 9810 9568    1762    3293                1776
      * 2       3    6353 8808 7684    2405    3516                7844
      */

    val rawTrainingData = sc.textFile("data/mllib/UCI/Wholesale_customers_data.csv")
    val rawData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {

        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).randomSplit(Array(0.8,0.2))

    // Cluster the data into two classes using KMeans
    val parsedTrainingData = rawData(0).cache()
    println("rawTrainingData is: " + rawTrainingData.count() + " ******* and parsedTrainingData is: " + parsedTrainingData.count())
    var clusterIndex: Int = 0
    val ks: Array[Int] = Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    ks.foreach(cluster => {
      val model: KMeansModel = KMeans.train(parsedTrainingData, cluster, 30, 1)
      val ssd = model.computeCost(parsedTrainingData)
      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> " + ssd)
    })
  }
  private def
  isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
// 做图查看K
//import matplotlib.pyplot as plt
//
//ks = [6.437427949853886E10
//,4.941713588415915E10
//,4.6604314103898865E10
//,3.738860439574063E10
//,3.853307800606712E10
//,2.788339580601425E10
//,2.8354371360074966E10
//,2.4122970684719566E10
//,2.2279411074495228E10
//,2.112924434530341E10
//,2.1217920186144577E10
//,1.907287594544136E10
//,1.7544210716086266E10
//,1.6831142774417511E10
//,1.4502332147368876E10
//,1.4835244623223576E10
//,1.345206366812804E10]
//k = range(3,20)
//for xy in zip(k,ks):
//plt.annotate("(%s,%s)" % xy, xy=xy, xytext=(-20, 10), textcoords='offset points')
//plt.plot(k,ks)
//plt.show()
//从上图的运行结果可以看到，当 K=9 时，cost 值有波动，但是后面又逐渐减小了，所以我们选择 8 这个临界点作为 K 的个数。当然可以多跑几次，找一个稳定的 K 值。
// 理论上 K 的值越大，聚类的 cost 越小，极限情况下，每个点都是一个聚类，这时候 cost 是 0，但是显然这不是一个具有实际意义的聚类结果。
