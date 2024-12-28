import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Main {
  // Define schema for the JSON payload
  val schema = StructType(
    Seq(
      StructField("symbol", StringType, false),
      StructField("timestamp", TimestampType, false),
      StructField("bids", ArrayType(ArrayType(StringType)), false),
      StructField("asks", ArrayType(ArrayType(StringType)), false)
    )
  )

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
      .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:example@localhost:27017"
      )
      .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://root:example@localhost:27017"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Kafka configuration
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "kafka:29092",
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

    // Calculate metrics
    // val bidAskSpread = computeBidAskSpread(parsedStream)
    // val marketDepth = computeMarketDepth(parsedStream)
    // val orderBookImbalance = computeOrderBookImbalance(parsedStream)
    // val marketTrends = computeMarketTrends(parsedStream)

    val tradeVolume = computeTradeVolume(parsedStream)
    val priceTrends = computePriceTrends(parsedStream)
    val volatility = computeVolatility(parsedStream)
    val vwap = computeVWAP(parsedStream)
    // Combine all metrics
    // val allMetrics = bidAskSpread
    //  .union(marketDepth)
    //  .union(orderBookImbalance)
    //  .union(marketTrends)

// Write tradeVolume to MongoDB
    tradeVolume.writeStream
      .format("mongo")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/tradeVolume")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("database", "crypto_analysis")
      .option("collection", "trade_volume")
      .start()

// Write priceTrends to MongoDB
    priceTrends.writeStream
      .format("mongo")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/priceTrends")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("database", "crypto_analysis")
      .option("collection", "price_trends")
      .start()

// Write volatility to MongoDB
    volatility.writeStream
      .format("mongo")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/volatility")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("database", "crypto_analysis")
      .option("collection", "volatility")
      .start()

// Write vwap to MongoDB
    vwap.writeStream
      .format("mongo")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/vwap")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("database", "crypto_analysis")
      .option("collection", "vwap")
      .start()

    spark.streams.awaitAnyTermination()
  }

  def computeBidAskSpread(
      orderBookDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    orderBookDF
      .withColumn("bid_price", expr("cast(bids[0][0] as double)"))
      .withColumn("ask_price", expr("cast(asks[0][0] as double)"))
      .withColumn("bid_ask_spread", col("ask_price") - col("bid_price"))
      .select(
        "symbol",
        "timestamp",
        "bid_price",
        "ask_price",
        "bid_ask_spread"
      )
      .withColumn("metric", lit("bid_ask_spread"))
  }

  def computeMarketDepth(
      orderBookDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    orderBookDF
      .withColumn(
        "total_bid_volume",
        expr("aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double))")
      )
      .withColumn(
        "total_ask_volume",
        expr("aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double))")
      )
      .select("symbol", "timestamp", "total_bid_volume", "total_ask_volume")
      .withColumn("metric", lit("market_depth"))
  }

  def computeOrderBookImbalance(
      orderBookDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    orderBookDF
      .withColumn(
        "total_bid_volume",
        expr("aggregate(bids, 0D, (acc, x) -> acc + cast(x[1] as double))")
      )
      .withColumn(
        "total_ask_volume",
        expr("aggregate(asks, 0D, (acc, x) -> acc + cast(x[1] as double))")
      )
      .withColumn(
        "order_book_imbalance",
        (col("total_bid_volume") - col("total_ask_volume")) / (col(
          "total_bid_volume"
        ) + col("total_ask_volume"))
      )
      .select("symbol", "timestamp", "order_book_imbalance")
      .withColumn("metric", lit("order_book_imbalance"))
  }

  def computeMarketTrends(
      orderBookDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    val windowSpec =
      Window.partitionBy("symbol").orderBy("tradeTime").rowsBetween(-5, 0)
    orderBookDF
      .withColumn("bid_price", expr("cast(bids[0][0] as double)"))
      .withColumn("ask_price", expr("cast(asks[0][0] as double)"))
      .withColumn("bid_ask_spread", col("ask_price") - col("bid_price"))
      .withColumn(
        "rolling_avg_spread",
        avg(col("bid_ask_spread")).over(windowSpec)
      )
      .select("symbol", "tradeTime", "rolling_avg_spread")
      .withColumn("metric", lit("market_trends"))
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
    val windowSpec = Window.partitionBy("symbol").orderBy("tradeTime")

    tradesDF
      .withColumn("prev_price", lag("price", 1).over(windowSpec))
      .withColumn("price_change", col("price") - col("prev_price"))
      .withColumn(
        "price_change_percent",
        (col("price_change") / col("prev_price")) * 100
      )
      .select(
        col("symbol"),
        col("tradeTime"),
        col("price"),
        col("prev_price"),
        col("price_change"),
        col("price_change_percent")
      )
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
