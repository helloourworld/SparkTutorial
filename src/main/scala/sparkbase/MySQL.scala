package main.scala.sparkbase

/**
  * Created by hadoop on 2016/12/1.
  */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object MySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Mysql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()
    // Creates a DataFrame based on a table named "people"
    // stored in a MySQL database.
    // classOf[com.mysql.jdbc.Driver]
    val url = "jdbc:mysql://192.168.71.128:3306/test?user=hive&password=hive"
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "pw")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts people by age
    val countsByAge = df.groupBy("pw").count()
    countsByAge.show()

    // Saves countsByAge to S3 in the JSON format.
    // countsByAge.write.format("json").save("/user/hadoop/a.json")
    // clean up
    sc.stop()
  }
}
