package main.scala.mllib.clusting

/**
  * Created by hadoop on 2016/12/15.
  * data/mllib/UCI/Wholesale_customers_data.csv 8 30 3
  * 0.8 6 30 3
  */

import java.sql.{DriverManager, ResultSet}

// import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._

object KMeansELKClustering {
  def main(args: Array[String]): Unit = {
    val userPrincipal = "admin"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/krb5.conf"
    val hadoopConf: Configuration = new Configuration()
    // LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("KMeansELKRDD").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // Load the driver
    //    classOf[org.postgresql.Driver]

    // Setup the connection
    val sourceURL = "jdbc:postgresql://171.0.11.6:25108/poc"
    val username = "omm"
    val password = "Gaussdba@Mpp"
    val url = "jdbc:postgresql://171.0.11.6:25108/poc?user=omm&password=Gaussdba@Mpp"
    DriverManager.setLoginTimeout(180)
    val conn = DriverManager.getConnection(sourceURL, username, password)
    // Configure to be Read Only
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    // Execute Query
    val result = statement.executeQuery("select stock_code,CURRENT_AMOUNT_BILLION,BUSINESS_AMOUNT_YEAR_BILLION,turnover_year,avg_business_price,business_freq_100 from for_cluster_01;")

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
        Vectors.dense(log10(line._2 / 1.0),log(line._4),log(line._5),log(line._6))
      }).randomSplit(Array(args(0).toDouble, 1.0 - args(0).toDouble))

    val schemaRaw = StructType(
      StructField("f1", FloatType) ::
        StructField("f2", FloatType)::
        StructField("f3", FloatType)
        :: Nil
    )
    val schemaData = resRDD.map{
    x =>
      Row.apply(x._1.toFloat,x._2.toFloat,x._3.toFloat)
    }
    val df2 = sqlContext.createDataFrame(schemaData, schemaRaw)
    // df2.show()
//    val normData = normalizer1.transform(df2.toDF())
    // Cluster the data into 6 classes using KMeans
    val parsedTrainingData = rawData(0).cache()
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val runTimes = args(3).toInt
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":" + x)
        clusterIndex += 1
      })
    // cluster to JDBC
    val schemaCenter = StructType(
      //StructField("predictedClusterIndex", StringType) ::
        StructField("x1", FloatType)::
        StructField("x2", FloatType)::
        StructField("x3", FloatType)::
        Nil
    )

    //begin to check which cluster each test data belongs to based on the clustering result
    val parsedTestData = rawData(0)
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })
    // 1 交叉评估，返回数据集和结果 textFile
    //    val result2 = resRDD.map {
    //      line =>
    //        val linevectore = Vectors.dense(line._2 / 1000000, line._3 / 1000000, line._4, line._5)
    //        val prediction = clusters.predict(linevectore)
    //        line + " " + prediction
    //    }.saveAsTextFile("data/cluster/result_10")
    // 2 交叉评估，返回数据集和结果 JDBC
    val schemaRes = StructType(
      StructField("stock_code", StringType) ::
        StructField("cluster_id", StringType)
        :: Nil
    )
    val data = resRDD.map {
      line =>
        val linevectore = Vectors.dense(log10(line._2 / 1.0),log(line._4),log(line._5),log(line._6))
        val prediction = clusters.predict(linevectore)
        Row.apply(line._1.toString, prediction.toString)
    }
    val df = sqlContext.createDataFrame(data, schemaRes)

    //df.collect()
    // df.createJDBCTable(url, "cluster_result3", false)
    println("Spark MLlib K-means clustering test finished.")
  }
}

//Cluster Number:6
//Cluster Centers Information Overview:
//Center Point of Cluster 0:[1.3949633683630636,3.343783349701716,2.0795672423951492,4.246059681224998]
//Center Point of Cluster 1:[0.6185521442321377,5.141012233292063,2.079128246477771,4.2563423671607525]
//Center Point of Cluster 2:[2.8065315949864353,0.09806156515043159,2.0796661445101,4.249780501706622]
//Center Point of Cluster 3:[1.0622813127918804,4.109131651996955,2.079547634890738,4.245499831553199]
//Center Point of Cluster 4:[1.7123122378886488,2.617394054211042,2.079635974166619,4.250935083411071]
//Center Point of Cluster 5:[2.057813078925439,1.826128561694778,2.078207066994107,4.25525838896666]

//0	207
//1	53
//2	18
//3	173
//4	206
//5	70