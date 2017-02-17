package main.scala.sparkbase.DataFrames

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
object FemaleInfoCollection2 {
  // Table structure, used for mapping the text data to df 
  case class FemaleInfo(name: String, gender: String, stayTime: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CreatingDataFrames").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession
      .builder()
      .appName("CreatingDataFrames")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()

    // Convert RDD to DataFrame through the implicit conversion, then register table.
//    sc.textFile(args(0)).map(_.split(","))
//      .map(p => FemaleInfo(p(0), p(1), p(2).trim.toInt))
//      .toDF.registerTempTable("FemaleInfoTable")

    val rawDF = sparkSession.read.option("header","false").option("inferSchema","true").csv(args(0)).coalesce(2)
    val colname = Seq("name", "gender", "stayTime")
    //     val n = rawDF.withColumnRenamed("_c0","name")
    //     n.printSchema()
    val dfRenamed = rawDF.toDF(colname: _*)
    dfRenamed.printSchema()
    dfRenamed.show()
//    val schema = StructType(Array(
//      StructField("name",StringType),
//      StructField("gender",StringType),
//      StructField("stayTime",IntegerType)
//    ))
//    val selectedDF = rawDF.select("_c0","_c1","_c2")
//    val transRDD = selectedDF.rdd.map(x => Row(x))
//    val Female = sparkSession.createDataFrame(transRDD,schema)
//    Female.show()
   dfRenamed.createOrReplaceTempView("FemaleInfoTable")
    // Via SQL statements to screen out the time information of female stay on the Internet , and aggregated the same names.
    val femaleTimeInfo = sparkSession.sql("select name,sum(stayTime) as " +
      "stayTime from FemaleInfoTable where gender = 'female' group by name")
    femaleTimeInfo.printSchema()

    // Filter information about female netizens who spend more than 2 hours online.
    val c = femaleTimeInfo.filter("stayTime >= 120").collect().foreach(println)
    sc.stop()  }
}
