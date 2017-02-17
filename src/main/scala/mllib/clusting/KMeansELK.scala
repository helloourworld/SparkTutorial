package main.scala.mllib.clusting

/**
  * Created by hadoop on 2017/1/9.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object KMeansELK {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansELK").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("KMeansELK")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val sourceUrl = "jdbc:mysql://192.168.71.128:3306/test?user=hive&password=hive"
    val df = spark
      .read
      .format("jdbc")
      .option("url", sourceUrl)
      .option("dbtable", "wholesales")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // print head Lines
    df.head(10).foreach(println)


    // clean up
    sc.stop()
  }
}
