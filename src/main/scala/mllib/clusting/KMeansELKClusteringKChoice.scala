package main.scala.mllib.clusting

import java.sql.{DriverManager, ResultSet}
import scala.math.{log10, log}
// import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Normalizer

object KMeansELKClusteringKChoice {
  def main(args: Array[String]) {
    // connect to FI and start Spark App
    val userPrincipal = "admin"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/krb5.conf"
    val hadoopConf: Configuration = new Configuration()
    // LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("KMeansELKClusteringKChoice").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // connect to ELK and read table
    val sourceURL = "jdbc:postgresql://171.0.11.6:25108/poc"
    val username = "omm"
    val password = "Gaussdba@Mpp"
    DriverManager.setLoginTimeout(180)
    val conn = DriverManager.getConnection(sourceURL, username, password)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query to read table for clustering
      val result = statement.executeQuery("select stock_code,CURRENT_AMOUNT_BILLION,BUSINESS_AMOUNT_YEAR_BILLION" +
        ",turnover_year,avg_business_price,business_freq_100 from for_cluster_01;")
      // Iterator for columns
      val resultSetList = Iterator.continually((result.next(), result)).takeWhile(_._1).map(r =>
        (r._2.getString(1),
          r._2.getDouble(2),
          r._2.getDouble(3),
          r._2.getDouble(4),
          r._2.getDouble(5),
          r._2.getInt(6)
          )
      ).toList
      val resRDD = sc.parallelize(resultSetList, 4)
      val normalizer1 = new Normalizer()
      val rawData =
        resRDD.map(line => {
          Vectors.dense(log10(line._2 / 1.0), log(line._4), log(line._5), log(line._6))
        }).randomSplit(Array(1.0, 0.0))
      // Cluster the data into classes using KMeans
      val parsedTrainingData = rawData(0).cache()
      println("rawTrainingData is: " + resRDD.count() + " ******* and parsedTrainingData is: " + parsedTrainingData.count())

      var clusterIndex: Int = 0
      val ks: Array[Int] = Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
      ks.foreach(cluster => {
        val model: KMeansModel = KMeans.train(parsedTrainingData, cluster, 30, 1)
        val ssd = model.computeCost(parsedTrainingData)
        println("sum of squared distances of points to their nearest center when k=" + cluster + " -> " + ssd)
      })
    }
    finally {
      conn.close()
    }
  }
}

//无监督， 每次的结果略有不同
//sum of squared distances of points to their nearest center when k=2 -> 386.40745309740123
//sum of squared distances of points to their nearest center when k=3 -> 237.443743366611
//sum of squared distances of points to their nearest center when k=4 -> 154.67362694516248
//sum of squared distances of points to their nearest center when k=5 -> 97.52694979505993
//sum of squared distances of points to their nearest center when k=6 -> 108.2131034679151
//sum of squared distances of points to their nearest center when k=7 -> 81.64476434998377
//sum of squared distances of points to their nearest center when k=8 -> 46.18486594541449
//sum of squared distances of points to their nearest center when k=9 -> 40.634099420663595
//sum of squared distances of points to their nearest center when k=10 -> 28.25715087924553
//sum of squared distances of points to their nearest center when k=11 -> 24.96389793327215
//sum of squared distances of points to their nearest center when k=12 -> 22.68020014277191
//sum of squared distances of points to their nearest center when k=13 -> 20.50167660288507
//sum of squared distances of points to their nearest center when k=14 -> 19.17152528621758
//sum of squared distances of points to their nearest center when k=15 -> 14.572742516020023
//sum of squared distances of points to their nearest center when k=16 -> 13.026859805353803
//sum of squared distances of points to their nearest center when k=17 -> 12.501805230923619
//sum of squared distances of points to their nearest center when k=18 -> 10.341842780399915
//sum of squared distances of points to their nearest center when k=19 -> 9.668225337655521
//sum of squared distances of points to their nearest center when k=20 -> 8.580566988679312