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

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")

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

    import spark.implicits._

    // Kafka configuration
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "kafka:29092",
      "subscribe" -> "orderbook_btcusdt,orderbook_ethusdt",
      "startingOffsets" -> "latest"
    )

    val kafkaStream = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

    // Parse Kafka JSON payload
    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(from_json("value", schema).as("data"))
      .select("data.*")

    // Calculate metrics
    val bidAskSpread = computeBidAskSpread(parsedStream)
    val marketDepth = computeMarketDepth(parsedStream)
    val orderBookImbalance = computeOrderBookImbalance(parsedStream)
    val marketTrends = computeMarketTrends(parsedStream)

    // Combine all metrics
    val allMetrics = bidAskSpread
      .union(marketDepth)
      .union(orderBookImbalance)
      .union(marketTrends)

    // Write results to MongoDB
    allMetrics.writeStream
      .format("mongo")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints")
      .trigger(Trigger.ProcessingTime("10 seconds"))
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
      Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-5, 0)
    orderBookDF
      .withColumn("bid_price", expr("cast(bids[0][0] as double)"))
      .withColumn("ask_price", expr("cast(asks[0][0] as double)"))
      .withColumn("bid_ask_spread", col("ask_price") - col("bid_price"))
      .withColumn(
        "rolling_avg_spread",
        avg(col("bid_ask_spread")).over(windowSpec)
      )
      .select("symbol", "timestamp", "rolling_avg_spread")
      .withColumn("metric", lit("market_trends"))
  }

  // Trade Volume and Activity Metrics
  def computeTradeVolume(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("timestamp", "5 minutes")
      .groupBy(window("timestamp", "1 minute"), "symbol")
      .agg(
        count("*").as("trade_count"),
        sum("quantity").as("total_trade_volume"),
        avg("quantity").as("avg_trade_size")
      )
      .select(
        "symbol",
        "window.start".as("start_time"),
        "window.end".as("end_time"),
        "trade_count",
        "total_trade_volume",
        "avg_trade_size"
      )
      .withColumn("metric", lit("trade_volume"))
  }

  // Price Trends (Rolling Averages and Momentum)
  def computePriceTrends(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    val windowSpec =
      Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-5, 0)

    tradesDF
      .withColumn("rolling_avg_price", avg("price").over(windowSpec))
      .withColumn(
        "price_momentum",
        "price" - lag("price", 1).over(windowSpec)
      )
      .select(
        "symbol",
        "timestamp",
        "rolling_avg_price",
        "price_momentum"
      )
      .withColumn("metric", lit("price_trends"))
  }

  // Volatility Analysis
  def computeVolatility(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("timestamp", "5 minutes")
      .groupBy(window("timestamp", "1 minute"), "symbol")
      .agg(
        stddev("price").as("price_volatility"),
        (max("price") - min("price")).as("price_range")
      )
      .select(
        "symbol",
        "window.start".as("start_time"),
        "window.end".as("end_time"),
        "price_volatility",
        "price_range"
      )
      .withColumn("metric", lit("volatility_analysis"))
  }

  // Volume-Weighted Average Price (VWAP)
  def computeVWAP(
      tradesDF: org.apache.spark.sql.DataFrame
  ): org.apache.spark.sql.DataFrame = {
    tradesDF
      .withWatermark("timestamp", "5 minutes")
      .groupBy(window("timestamp", "1 minute"), "symbol")
      .agg(
        sum("price" * "quantity").as("price_volume"),
        sum("quantity").as("total_volume")
      )
      .withColumn("vwap", "price_volume" / "total_volume")
      .select(
        "symbol",
        "window.start".as("start_time"),
        "window.end".as("end_time"),
        "vwap"
      )
      .withColumn("metric", lit("vwap"))
  }
}
