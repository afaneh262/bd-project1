import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter
import java.io._

import org.apache.spark.util.sketch.BloomFilter

object BloomFilterManager extends Serializable {
  @transient private var bloomFilter: BloomFilter = null

  def getInstance: BloomFilter = {
    if (bloomFilter == null) {
      synchronized {
        if (bloomFilter == null) {
          bloomFilter = BloomFilter.create(1000000, 0.01)
        }
      }
    }
    bloomFilter
  }

  def checkAndAdd(tradeId: Int): Boolean = {
    val filter = getInstance
    synchronized {
      if (!filter.mightContain(tradeId)) {
        println("Adding tradeId to BloomFilter: " + tradeId)
        filter.put(tradeId)
        true
      } else {
        println("TradeId already exists in BloomFilter: " + tradeId)
        false
      }
    }
  }
}

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

    val checkTradeIdUDF =
      udf((tradeId: Int) => BloomFilterManager.checkAndAdd(tradeId))

    val allCoins = Seq(
      "BTCUSDT",
      "ETHUSDT",
      "BNBUSDT",
      "ADAUSDT",
      "XRPUSDT",
      "DOGEUSDT",
      "DOTUSDT",
      "UNIUSDT",
      "LINKUSDT",
      "LTCUSDT",
      "BCHUSDT",
      "SOLUSDT",
      "MATICUSDT",
      "XLMUSDT",
      "ETCUSDT",
      "THETAUSDT",
      "VETUSDT",
      "TRXUSDT",
      "EOSUSDT",
      "FILUSDT",
      "AAVEUSDT",
      "XTZUSDT",
      "ATOMUSDT",
      "NEOUSDT",
      "ALGOUSDT",
      "KSMUSDT",
      "MKRUSDT",
      "COMPUSDT",
      "CROUSDT",
      "FTTUSDT",
      "ICPUSDT",
      "AVAXUSDT",
      "BTTUSDT",
      "CAKEUSDT"
    )

    val topics =
      allCoins.map(coin => s"trades_${coin.toLowerCase()}").mkString(",")

    // Kafka configuration
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> topics,
      "startingOffsets" -> "latest"
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

    val filteredStream = parsedStream
      .filter(checkTradeIdUDF(col("tradeId")))

    println("reading from kafka")
    val tradeVolume = computeTradeVolume(filteredStream)
    val priceTrends = computePriceTrends(filteredStream)
    val volatility = computeVolatility(filteredStream)
    val vwap = computeVWAP(filteredStream)

    // Write trade volume to MongoDB
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

    // Write priceTrends to MongoDB
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

    // Write volatility to MongoDB
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

    // Write vwap to MongoDB
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
        col("window.start").cast("long").as("timestamp"),
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
        col("window.start").cast("long").as("timestamp"),
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
        col("window.start").cast("long").as("timestamp"),
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
        col("window.start").cast("long").as("timestamp"),
        col("vwap")
      )
      .withColumn("metric", lit("vwap"))
  }
}
