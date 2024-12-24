package com.crypto.prediction

class PriceAnalyzer(
    bloomSize: Int = 10000,
    bloomHashFunctions: Int = 3,
    cmsWidth: Int = 1000,
    cmsDepth: Int = 5
) {

  private val priceBloomFilter =
    new BloomFilterTracker(bloomSize, bloomHashFunctions)
  private val patternSketch = new CountMinSketch(cmsWidth, cmsDepth)

  // Track unique price levels
  def trackPrice(price: Double): Boolean = {
    val isNewPrice = !priceBloomFilter.mightContain(price)
    if (isNewPrice) {
      priceBloomFilter.add(price)
    }
    isNewPrice
  }

  // Track price movement patterns
  def trackPricePattern(pattern: String): Long = {
    patternSketch.add(pattern)
    patternSketch.estimateCount(pattern)
  }

  // Generate price pattern string from recent prices
  def generatePricePattern(prices: Seq[Double]): String = {
    if (prices.length < 2) {
      return ""
    }
    prices
      .sliding(2)
      .map { case Seq(a, b) =>
        if (b > a) {
          "U"
        } else if (b < a) {
          "D"
        } else {
          "S"
        }
      }
      .mkString
  }
}
