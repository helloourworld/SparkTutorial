package main.scala.mllib.ml.CF

/**
  * Created by hadoop on 2016/11/23.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object MovieLensALS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 1.0 装载样本评分数据，其中最后一列时间戳除10的余数作为key，Rating为值；
    // Load and parse the data
    val data = sc.textFile("data/ml-1m/ratings.dat")
    val ratings = data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      (timestamp.toLong % 10,Rating(user.toInt, item.toInt, rate.toDouble))
    }) //注意ratings是[(Long, org.apache.spark.mllib.recommendation.Rating)]

    // 1.1 装载待推荐用户评分数据,为该用户训练评分参数；
    def loadRatings(path: String): Seq[Rating] = {

      val lines = sc.textFile(path)
      val ratings = lines.map { line =>
        val fields = line.split("::")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }.filter(_.rating > 0.0).collect()
      if (ratings.isEmpty) {
        sys.error("No ratings provided.")
      } else {
        ratings.toSeq
      }}

    val myRatings = loadRatings("data/movielens/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    // val data = sc.textFile("data/mllib/als/test.data")
    // val ratings = data.map(_.split(",") match { case Array(user, item, rate, timestamp) =>
    //    (timestamp.toLong % 10,Rating(user.toInt, item.toInt, rate.toDouble))
    //  }) //注意ratings是[(Long, org.apache.spark.mllib.recommendation.Rating)]

    // 电影目录对照表（电影ID->电影标题）

    val moviesRDD = sc.textFile("data/ml-1m/movies.dat").map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }.cache()
    val movies = moviesRDD.collect().toMap

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    // 评介前30部电影

    val mean_rating = ratings.map(_._2.rating).reduce(_ + _) /numRatings.toDouble
    val max_rating = ratings.map(_._2.rating).max
    val min_rating = ratings.map(_._2.rating).min
    val mod_rating = ratings.map(s => (s._2.rating,1)).reduceByKey(_ + _).map(x => (x._2,x._1)).top(3)

    val topn = 10
    println("评介次数前" + topn + "部电影: ")// val high_rating =
    val rating_num = ratings.map(s => (s._2.product,1)).reduceByKey(_ + _).cache()
    val rating_above1000 = rating_num.filter(_._2 > 1000)
    val most_rating = rating_num.join(moviesRDD).map(x => (x._2._1,x._2._2)).top(topn).map(x => (x._2,x._1))
    var n = 1
    most_rating.foreach { m =>
      println("%2d".format(n) + ": " + "%60s".format(m._1) + "---->" + "%5d".format(m._2))
      n += 1
    }


    println("\n评介分数前" + topn + "部电影: ")
    val high_rating1 = ratings.map(s => (s._2.product,s._2.rating)).groupByKey().map{x => (x._1,x._2.reduce(_ + _)/x._2.count(l => true))}.join(moviesRDD).map(x => (x._2._1,x._2._2)).top(topn).map(x => (x._2,x._1))
    var s1 = 1
    high_rating1.foreach { h =>
      println("%2d".format(s1) + ": " + "%60s".format(h._1) + "---->" + "%5.2f".format(h._2))
      s1 += 1
    }

    println("超1000电影数: " + rating_above1000.count()+"中评分前" + topn + "电影: ")
    val high_rating = ratings.map(s => (s._2.product,s._2.rating)).groupByKey().map{x => (x._1,x._2.reduce(_ + _)/x._2.count(l => true))}.rightOuterJoin(rating_above1000).map(x => (x._1,x._2._1.get)).join(moviesRDD).map(x => (x._2._1,x._2._2)).top(topn).map(x => (x._2,x._1))
    var s2 = 1
    high_rating.foreach { h =>
      println("%2d".format(s2) + ": " + "%60s".format(h._1) + "---->" + "%5.2f".format(h._2))
      s2 += 1
    }


    // 2 将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
    // 数据在计算过程中要多次使用，cache到内存
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6).values.union(myRatingsRDD).repartition(numPartitions).cache()
    val validation = ratings.filter(x => x._1 >=6 & x._1 < 8).values.repartition(numPartitions).cache()
    val test = ratings.filter(x => x._1 >= 8).values.repartition(numPartitions).cache()

    // 样本数据explore
    training.sample(false, 0.0001,12345678).collect()

    // 样本比例
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // 3 训练不同参数下的模型，并再校验集中验证，获取最佳参数下的模型
    // Build the recommendation model using ALS

    // numBlocks 是用于并行化计算的分块个数（设置为-1时 为自动配置）；
    // rank是模型中隐性因子的个数；
    // iterations是迭代的次数；
    // lambda是ALS 的正则化参数；
    // implicitPrefs决定了是用显性反馈ALS 的版本还是用隐性反馈数据集的版本；
    // alpha是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准。

    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIterations = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRMSE = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIterations = -1

    for (rank <- ranks; lambda <- lambdas; numIteration <- numIterations){

      // val model = ALS.train(ratings, rank, numIterations, 0.01)
      val start=System.currentTimeMillis()
      val model = ALS.train(training, rank, numIteration, lambda)
      println("Train spended "+(System.currentTimeMillis()-start)/1000+"s")
      val validationRMSE = computeRMSE(model, validation)
      println("RMSE (validation) = " + validationRMSE + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIteration = " + numIteration + ".")
      if (validationRMSE < bestValidationRMSE) {
        bestModel = Some(model)
        bestValidationRMSE = validationRMSE
        bestRank = rank
        bestLambda = lambda
        bestNumIterations = numIteration
      }
    }

    // 4 用最佳模型预测测试集的评分，计算和实际评分之间的均方根误差
    val testRmse = computeRMSE(bestModel.get, test)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda  + ", and numIter = " + bestNumIterations + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model
    val meanRating = training.union(validation).map(_.rating).mean

    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)

    val improvement = -( testRmse - baselineRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // 5 根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）
    // 推荐电影
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((0,_)))
      .collect()
      .sortBy(-_.rating)
      .take(10)


    println("Movies recommended for you:")
    var r = 1
    recommendations.foreach { recom =>
      println("%2d".format(r) + ": " + movies(recom.product))
      r += 1
    }
    println()
    // clean up
    sc.stop()
  }
  // compute 均方根误差(RMSE)
  // Evaluate the model on rating data
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val usersProducts = data.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    val ratesAndPreds = data.map { case Rating(user, product, rate) =>
      ((user, product), rate)}.join(predictions)

    val RMSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    math.sqrt(RMSE)
    // println("Mean Squared Error = " + RMSE)
  }
}
