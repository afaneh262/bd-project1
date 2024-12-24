package com.crypto.prediction

import scala.collection.mutable.BitSet
import java.security.MessageDigest

class BloomFilterTracker(size: Int, numHashFunctions: Int) {
  private val bitSet = BitSet(size)
  private val md = MessageDigest.getInstance("MD5")

  def add(pricePoint: Double): Unit = {
    val hashValues = getHashValues(pricePoint)
    hashValues.foreach(h => bitSet.add(h % size))
  }
  
  def mightContain(pricePoint: Double): Boolean = {
    val hashValues = getHashValues(pricePoint)
    hashValues.forall(h => bitSet.contains(h % size))
  }

  private def getHashValues(pricePoint: Double): Array[Int] = {
    val bytes = pricePoint.toString.getBytes
    val hash = md.digest(bytes)
    
    (0 until numHashFunctions).map { i =>
      // Use different segments of the hash for each function
      val hashValue = hash.slice(i * 4, (i + 1) * 4).foldLeft(0)((a, b) => (a << 8) + (b & 0xff))
      Math.abs(hashValue)
    }.toArray
  }
}