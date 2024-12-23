package com.crypto.prediction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Main {
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

    val topics =
      Array("orderbook_btcusdt", "orderbook_ethusdt", "orderbook_bnbusdt")

    // Create DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Process stream
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Convert RDD to DataFrame
        val orderBookDF = spark.read.json(rdd.map(_.value()))

        // Feature Engineering
        val featureDF = orderBookDF
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

        // Prepare features for ML
        val assembler = new VectorAssembler()
          .setInputCols(Array("bid_ask_ratio", "weighted_mid_price"))
          .setOutputCol("features")

        // Define the model
        val rf = new RandomForestRegressor()
          .setLabelCol("weighted_mid_price")
          .setFeaturesCol("features")
          .setPredictionCol("prediction")

        // Create and fit pipeline
        val pipeline = new Pipeline().setStages(Array(assembler, rf))
        val model = pipeline.fit(featureDF)

        // Make predictions
        val predictions = model
          .transform(featureDF)
          .select("symbol", "timestamp", "weighted_mid_price", "prediction")

        // Save to MongoDB
        predictions.write
          .format("mongo")
          .mode("append")
          .save()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
