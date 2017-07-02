package main.scala.mllib

/**
  * Created by hadoop on 2017/4/6.
  * http://stackoverflow.com/questions/41416291/how-to-prepare-data-into-a-libsvm-format-from-dataframe?rq=1
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object ConvertToLibsvm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConvertToLibsvm").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawRatings: Seq[String] = Seq("0,1,1.0", "0,3,3.0", "1,1,1.0", "1,2,0.0", "1,3,3.0", "3,3,4.0", "10,3,4.5")
    val data: RDD[MatrixEntry] =
      sc.parallelize(rawRatings).map {
        line => {
          val fields = line.split(",")
          val i = fields(0).toLong
          val j = fields(1).toLong
          val value = fields(2).toDouble
          MatrixEntry(i, j, value)
        }
      }
    // Now let's convert that RDD[MatrixEntry] to a CoordinateMatrix and extract the indexed rows :
    val df = new CoordinateMatrix(data) // Convert the RDD to a CoordinateMatrix
      .toIndexedRowMatrix().rows // Extract indexed rows
      //.toDF("label", "features") // Convert rows
    // TODO:?? something wrong
    df.foreach(println)
    // clean up
    sc.stop()
  }
}
