package com.crypto.prediction

import scala.util.hashing.MurmurHash3
import scala.collection.mutable.ArrayBuffer

class CountMinSketch(width: Int, depth: Int) {
  private val sketch = Array.ofDim[Long](depth, width)
  private val hashFunctions = (0 until depth).map(i => (key: String) => 
    Math.abs(MurmurHash3.stringHash(key + i.toString)) % width)

  def add(key: String, count: Long = 1): Unit = {
    hashFunctions.zipWithIndex.foreach { case (hash, row) =>
      val col = hash(key)
      sketch(row)(col) += count
    }
  }

  def estimateCount(key: String): Long = {
    hashFunctions.zipWithIndex.map { case (hash, row) =>
      sketch(row)(hash(key))
    }.min
  }
}