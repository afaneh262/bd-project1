package com.crypto.prediction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{RandomForestRegressor, GBTRegressor}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.ml.evaluation.RegressionEvaluator
import scala.collection.mutable

object Main {
  // Window duration for trade aggregation
  val WINDOW_DURATION = "1 minute"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CryptoPrediction")
      .config(
        "spark.mongodb.output.uri",
        "mongodb://mongodb:27017/crypto.predictions"
      )
      .getOrCreate()

    import spark.implicits._

    // Create Streaming Context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    // Kafka configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:29092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "crypto_prediction_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Subscribe to both order book and trade topics
    val symbols = Array("btcusdt", "ethusdt", "bnbusdt")
    val orderBookTopics = symbols.map(s => s"orderbook_$s")
    val tradeTopics = symbols.map(s => s"trades_$s")
    val allTopics = orderBookTopics ++ tradeTopics

    // Create DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](allTopics, kafkaParams)
    )

    // Process stream
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Split order book and trade data
        val orderBookRDD =
          rdd.filter(record => record.topic().startsWith("orderbook_"))
        val tradesRDD =
          rdd.filter(record => record.topic().startsWith("trades_"))

        // Convert RDDs to DataFrames
        val orderBookDF = spark.read.json(orderBookRDD.map(_.value()))
        val tradesDF = spark.read.json(tradesRDD.map(_.value()))

        // Process order book features
        val orderBookFeatures = processOrderBook(orderBookDF)

        // Process trade features
        val tradeFeatures = processTrades(tradesDF)

        // Join features and prepare for training
        val combinedFeatures = joinFeatures(orderBookFeatures, tradeFeatures)

        if (!combinedFeatures.isEmpty) {
          // Train and predict
          trainAndPredict(combinedFeatures, spark)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processOrderBook(
      orderBookDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    orderBookDF
      .withColumn(
        "bid_ask_ratio",
        expr(
          "aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc) / " +
            "aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc)"
        )
      )
      .withColumn(
        "weighted_mid_price",
        expr(
          "(aggregate(bids, 0D, (acc, x) -> acc + cast(x[0] as double) * cast(x[1] as double), acc -> acc) + " +
            "aggregate(asks, 0D, (acc, x) -> acc + cast(x[0] as double) * cast(x[1] as double), acc -> acc)) / " +
            "(aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc) + " +
            "aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc))"
        )
      )
      .withColumn(
        "spread",
        expr("cast(asks[0][0] as double) - cast(bids[0][0] as double)")
      )
      .withColumn(
        "order_book_imbalance",
        expr(
          "(aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc) - " +
            "aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc)) / " +
            "(aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc) + " +
            "aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double), acc -> acc))"
        )
      )
      .select(
        "symbol",
        "timestamp",
        "bid_ask_ratio",
        "weighted_mid_price",
        "spread",
        "order_book_imbalance"
      )
  }

  def processTrades(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("timestamp", WINDOW_DURATION)
      .groupBy(
        window(col("timestamp"), WINDOW_DURATION),
        col("symbol")
      )
      .agg(
        avg("price").as("avg_price"),
        sum("quantity").as("volume"),
        count("*").as("trade_count"),
        stddev("price").as("price_volatility"),
        (max("price") - min("price")).as("price_range")
      )
      .select(
        "symbol",
        "window.end".as("timestamp"),
        "avg_price",
        "volume",
        "trade_count",
        "price_volatility",
        "price_range"
      )
  }

  def joinFeatures(
      orderBookFeatures: org.apache.spark.sql.DataFrame,
      tradeFeatures: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    orderBookFeatures
      .join(
        tradeFeatures,
        Seq("symbol", "timestamp"),
        "outer"
      )
      .na
      .fill(0) // Fill missing values with 0
  }

  // Map of price analyzers for each symbol
  private val priceAnalyzers =
    new scala.collection.mutable.HashMap[String, PriceAnalyzer]()

  def trainAndPredict(
      features: org.apache.spark.sql.DataFrame,
      spark: SparkSession
  ): Unit = {
    // Initialize price analyzers for each symbol if not exists
    features.select("symbol").distinct().collect().foreach { row =>
      val symbol = row.getString(0)
      if (!priceAnalyzers.contains(symbol)) {
        priceAnalyzers(symbol) = new PriceAnalyzer()
      }
    }

    // Prepare features for ML
    val featureCols = Array(
      "bid_ask_ratio",
      "spread",
      "order_book_imbalance",
      "volume",
      "trade_count",
      "price_volatility",
      "price_range"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("assembled_features")

    val scaler = new StandardScaler()
      .setInputCol("assembled_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    // Define the models
    val rf = new RandomForestRegressor()
      .setLabelCol("weighted_mid_price")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)

    val gbt = new GBTRegressor()
      .setLabelCol("weighted_mid_price")
      .setFeaturesCol("features")
      .setMaxIter(100)

    // Create and fit pipelines
    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    val gbtPipeline = new Pipeline().setStages(Array(assembler, scaler, gbt))

    // Split data for training
    val Array(trainingData, testData) = features.randomSplit(Array(0.8, 0.2))

    // Train both models
    val rfModel = rfPipeline.fit(trainingData)
    val gbtModel = gbtPipeline.fit(trainingData)

    // Make predictions with both models
    val rfPredictions = rfModel.transform(testData)
    val gbtPredictions = gbtModel.transform(testData)

    // Evaluate models
    val evaluator = new RegressionEvaluator()
      .setLabelCol("weighted_mid_price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rfRmse = evaluator.evaluate(rfPredictions)
    val gbtRmse = evaluator.evaluate(gbtPredictions)

    // Use the better model for final predictions
    val finalPredictions =
      if (rfRmse < gbtRmse) rfPredictions else gbtPredictions

    // Save predictions to MongoDB
    finalPredictions
      .select("symbol", "timestamp", "weighted_mid_price", "prediction")
      .write
      .format("mongo")
      .mode("append")
      .save()
  }
}
