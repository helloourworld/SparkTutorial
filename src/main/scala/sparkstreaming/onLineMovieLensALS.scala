package main.scala.sparkstreaming

/**
  * Created by hadoop on 2016/11/23.
  * 30 30 30 movie h1,h2,h3
  * first run movieKafka.py for sending user's ratings in
  *
  */

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.math.abs
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object onLineMovieLensALS {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println("Usage: batchTime, windowTime, slideTime, topic, brokers")
      System.exit(1)
    }
    val Array(batchTime, windowTime, slideTime, topic, brokers) = args
    val batchDuration = Seconds(batchTime.toInt)
    val windowDuration = Seconds(windowTime.toInt)
    val slideDuration = Seconds(slideTime.toInt)

    //每隔20秒计算一批数据local[4]：意思本地起4个进程运行，setAppName("SparkStreaming")：设置运行处理类
    val conf = new SparkConf().setAppName("onLineMovieLensALS").setMaster("local[2]")
    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint("root")

    // 1.0 装载样本评分数据，其中最后一列时间戳除10的余数作为key，Rating为值；
    // Load and parse the data
    val data = ssc.sparkContext.textFile("data/ml-1m/ratings.dat")
    val ratings = data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      (timestamp.toLong % 10,Rating(user.toInt, item.toInt, rate.toDouble))
    }) //注意ratings是[(Long, org.apache.spark.mllib.recommendation.Rating)]

    // 1.1 电影目录对照表（电影ID->电影标题）
    val moviesRDD = ssc.sparkContext.textFile("data/ml-1m/movies.dat").map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }.cache()
    val movies = moviesRDD.collect().toMap

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")


     // 2 将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
     // 数据在计算过程中要多次使用，cache到内存
     val numPartitions = 4
     val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
     val validation = ratings.filter(x => x._1 >=6 & x._1 < 8).values.repartition(numPartitions).cache()
     val test = ratings.filter(x => x._1 >= 8).values.repartition(numPartitions).cache()

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

    val ranks = List(12)
    val lambdas = List(0.1)
    val numIterations = List(20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRMSE = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIterations = -1

    for (rank <- ranks; lambda <- lambdas; numIteration <- numIterations){
      // val model = ALS.train(ratings, rank, numIterations, 0.01)
      val model = ALS.train(training, rank, numIteration, lambda)
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

    // 5 装载待推荐用户评分数据,为该用户训练评分参数；本部分使用Kafka流得到
    //     Get the list of topic used by kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "movieLens",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val myRatingsRDD = stream.map(x => x.value).flatMap(_.split("&&")).map(_.split("::")).map{
      fields => Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0).window(windowDuration,slideDuration).cache()

    myRatingsRDD.foreachRDD(rdd => rdd.foreach(println))

    var allData= training.union(validation)
    allData.cache().count()
    training.unpersist()
    validation.unpersist()

    var index=0
    myRatingsRDD.foreachRDD { rdd =>
      index += 1
      println("[Info: Round  " + index + " received data number:" + rdd.count())
      val tmpData = rdd.union(allData)
      tmpData.cache().count()
      allData = tmpData
      tmpData.unpersist()
      val startTrain = System.currentTimeMillis()
      for (rank <- ranks; lambda <- lambdas; numIteration <- numIterations) {
        val model2 = ALS.train(allData, rank, numIteration, lambda)
        println("[Info: Round  " + index + "] train spended time:" + (System.currentTimeMillis() - startTrain) / 1000 + "s")
        val meanRating2 = allData.map(_.rating).mean
        val baselineRmse2 = math.sqrt(test.map(x => (meanRating2 - x.rating) * (meanRating2 - x.rating)).mean)
        val rmse = computeRMSE(model2, test)
        println("[Info: Round  " + index + "] baselinermse:" + baselineRmse2)
        println("[Info: Round  " + index + "] rmse:" + rmse)
        val improvement2 = (baselineRmse2 - rmse) / baselineRmse2 * 100
        println("[Info: Round  " + index + "] model improve:" + "%1.2f".format(improvement) + "%.")
        if (rmse < bestValidationRMSE) {
          bestModel = Some(model2)
          bestValidationRMSE = rmse
        }
        println("===================================")
      }
      // 6 根据用户评分的数据，推荐前十部最感兴趣的电影（注意要剔除用户已经评分的电影）
      val myRatedMovieIds = rdd.map(_.product)//.collect().toSet
      // myRatedMovieIds.foreach(println)
      val candidates = moviesRDD.map(x => x._1).subtract(myRatedMovieIds)
      //val candidates = movies.keys.filterNot(myRatedMovieIds.contains(_)).toSeq
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
      println("*" * 50)
    }
    // clean up
    println("*" * 20 + "start recommend:" + "*" * 20)
    ssc.start()
    ssc.awaitTermination()
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
