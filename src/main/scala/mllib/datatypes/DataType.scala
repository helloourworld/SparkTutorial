package main.scala.mllib.datatypes

/**
  * Created by hadoop on 2016/11/17.
  */

// Note: Scala imports scala.collection.immutable.Vector by default, so you have to i
// mport org.apache.spark.mllib.linalg.Vector explicitly to use MLlib’s Vector.
/*
MLlib recognizes the following types as dense vectors:

NumPy’s array
Python’s list, e.g., [1, 2, 3]
and the following as sparse vectors:

MLlib’s SparseVector.
SciPy’s csc_matrix with a single column
*/

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

object DataType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataType").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Create a dense vector (1.0, 0.0, 3.0).
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))


  }
}
