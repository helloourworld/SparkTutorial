package main.scala.sparkbase.DataFrames

/**
  * Created by hadoop on 2016/12/28.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object CreatingDataFrames {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreatingDataFrames").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("CreatingDataFrames")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // clean up
    sc.stop()
  }
}

//# spark is an existing SparkSession
//df = spark.read.json("examples/src/main/resources/people.json")
//# Displays the content of the DataFrame to stdout
//df.show()
//# +----+-------+
//# | age|   name|
//# +----+-------+
//# |null|Michael|
//# |  30|   Andy|
//# |  19| Justin|
//# +----+-------+

//df <- read.json("examples/src/main/resources/people.json")
//
//# Displays the content of the DataFrame
//head(df)
//##   age    name
//## 1  NA Michael
//## 2  30    Andy
//## 3  19  Justin
//
//# Another method to print the first few rows and optionally truncate the printing of long values
//showDF(df)
//## +----+-------+
//## | age|   name|
//## +----+-------+
//## |null|Michael|
//## |  30|   Andy|
//## |  19| Justin|
//## +----+-------+