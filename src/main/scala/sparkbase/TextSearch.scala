package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/1.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TextSearch {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TextSearch")
    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames

    val textFile = sc.textFile("/user/hadoop/README.md")
    // Creates a DataFrame having a single column named "line"
    val df = textFile.toDF("line")

//    val errors = df.filter(col("line").like("%ERROR%"))
//    // Counts all the errors
//    errors.count()
//    // Counts errors mentioning MySQL
//    errors.filter(col("line").like("%MySQL%")).count()
//    // Fetches the MySQL errors as an array of strings
//    errors.filter(col("line").like("%MySQL%")).collect()
    // clean up
    sc.stop()
  }
}
