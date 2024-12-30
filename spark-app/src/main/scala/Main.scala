import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Main {
  val mongoUri = "mongodb://root:example@localhost:27017"

  // Define schema for the JSON payload
  val tradeSchema = StructType(
    Seq(
      StructField("tradeTime", TimestampType, false),
      StructField("symbol", StringType, false),
      StructField("price", StringType, false),
      StructField("quantity", StringType, false),
      StructField("isBuyerMaker", BooleanType, false),
      StructField("maker", BooleanType, false),
      StructField("tradeId", IntegerType, false)
    )
  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Task-1")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", mongoUri)
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Kafka configuration
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "trades_bnbusdt,trades_btcusdt,trades_ethusdt",
      "startingOffsets" -> "earliest"
    )

    val kafkaStream = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

    // Parse Kafka JSON payload
    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), tradeSchema).as("data"))
      .select("data.*")
      .withColumn("price", col("price").cast(DoubleType))
      .withColumn("quantity", col("quantity").cast(DoubleType))

    val tradeVolume = computeTradeVolume(parsedStream)
    val priceTrends = computePriceTrends(parsedStream)
    val volatility = computeVolatility(parsedStream)
    val vwap = computeVWAP(parsedStream)

    tradeVolume.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          batchDF.write
            .format("mongo")
            .mode("append")
            .option("uri", mongoUri)
            .option("database", "crypto_analysis")
            .option("collection", "trade_volume")
            .save()
          println("tradeVolume metrics written to MongoDB", batchId)
      }
      .outputMode("update")
      .option("checkpointLocation", "/tmp/checkpoints/tradeVolume")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Write priceTrends to MongoDB using foreachBatch
    priceTrends.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          batchDF.write
            .format("mongo")
            .mode("append")
            .option("uri", mongoUri)
            .option("database", "crypto_analysis")
            .option("collection", "price_trends")
            .save()
          println("priceTrends metrics written to MongoDB", batchId)
      }
      .outputMode("update")
      .option("checkpointLocation", "/tmp/checkpoints/priceTrends")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Write volatility to MongoDB using foreachBatch
    volatility.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          batchDF.write
            .format("mongo")
            .mode("append")
            .option("uri", mongoUri)
            .option("database", "crypto_analysis")
            .option("collection", "volatility")
            .save()
          println("volatility metrics written to MongoDB", batchId)
      }
      .outputMode("update")
      .option("checkpointLocation", "/tmp/checkpoints/volatility")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Write vwap to MongoDB using foreachBatch
    vwap.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          batchDF.write
            .format("mongo")
            .mode("append")
            .option("uri", mongoUri)
            .option("database", "crypto_analysis")
            .option("collection", "vwap")
            .save()
          println("VWAP metrics written to MongoDB", batchId)
      }
      .outputMode("update")
      .option("checkpointLocation", "/tmp/checkpoints/vwap")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    spark.streams.awaitAnyTermination()
  }

  // Trade Volume and Activity Metrics
  def computeTradeVolume(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("tradeTime", "5 minutes")
      .groupBy(window(col("tradeTime"), "1 minute"), col("symbol"))
      .agg(
        count("*").as("trade_count"),
        sum("quantity").as("total_trade_volume"),
        avg("quantity").as("avg_trade_size")
      )
      .select(
        col("symbol"),
        col("window.start").as("start_time"),
        col("window.end").as("end_time"),
        col("trade_count"),
        col("total_trade_volume"),
        col("avg_trade_size")
      )
      .withColumn("metric", lit("trade_volume"))
  }

  // Price Trends (Rolling Averages and Momentum)
  def computePriceTrends(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("tradeTime", "5 minutes")
      .groupBy(
        window(col("tradeTime"), "1 minute"),
        col("symbol")
      )
      .agg(
        avg("price").cast(DoubleType).as("rolling_avg_price"),
        (max("price").cast(DoubleType) - min("price").cast(DoubleType))
          .as("price_momentum")
      )
      .select(
        col("symbol"),
        col("window.start").as("start_time"),
        col("window.end").as("end_time"),
        col("rolling_avg_price"),
        col("price_momentum")
      )
      .withColumn("metric", lit("price_trends"))
  }

  // Volatility Analysis
  def computeVolatility(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("tradeTime", "5 minutes")
      .groupBy(window(col("tradeTime"), "1 minute"), col("symbol"))
      .agg(
        stddev("price").as("price_volatility"),
        (max("price") - min("price")).as("price_range")
      )
      .select(
        col("symbol"),
        col("window.start").as("start_time"),
        col("window.end").as("end_time"),
        col("price_volatility"),
        col("price_range")
      )
      .withColumn("metric", lit("volatility_analysis"))
  }

  // Volume-Weighted Average Price (VWAP)
  def computeVWAP(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("tradeTime", "5 minutes")
      .groupBy(window(col("tradeTime"), "1 minute"), col("symbol"))
      .agg(
        sum(col("price") * col("quantity").cast("double")).as("price_volume"),
        sum("quantity").as("total_volume")
      )
      .withColumn("vwap", expr("price_volume / total_volume"))
      .select(
        col("symbol"),
        col("window.start").alias("start_time"),
        col("window.end").as("end_time"),
        col("vwap")
      )
      .withColumn("metric", lit("vwap"))
  }
}
