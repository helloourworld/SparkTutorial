package main.scala.sparkbase

/**
  * Created by hadoop on 2017/2/6.
  * HIGH PERFORMANCE LINEAR ALGEBRA http://fommil.com/scalax14/#/
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Breeze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Breeze").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Breeze")
      .config("spark.sql.warehouse.dir", "file:///d:/root/")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    import breeze.linalg._

    val x = DenseVector.zeros[Double](5)
    println(x)

    val xt = x.t
    println(xt)

    val m = DenseMatrix.zeros[Int](5,5)
    println(m)

    println((m.rows, m.cols))

    println(m(::,1))

    m(4,::) := DenseVector(1,2,3,4,5).t
    println(m(4,::))
    println(m)

    // clean up
    sc.stop()
  }
}

//Operation	Example
//Elementwise addition	a + b
//Elementwise multiplication	a :* b
//Vector dot product	a dot b
//Elementwise sum	sum(a)
//Elementwise max	a.max
//Elementwise argmax	argmax(a)
//Linear solve	a  b
//Transpose	a.t
//Determinant	det(a)
//Inverse	inv(a)
//Pseudoinverse	pinv(a)
//SVD	SVD(u,s,v) = svd(a)