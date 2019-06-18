package com.tencent.angel.spark.ml.graph.utils

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD

object Degrees {

  def inDegrees(edges: RDD[(Int, Int)], numPartitions: Int): RDD[(Int, Int)] = {
    edges.mapPartitions { case iter =>
      val map = new Int2IntOpenHashMap()
      iter.foreach { case (_, dst) =>
        map.addTo(dst, 1)
      }

      val arr = new Array[(Int, Int)](map.size())
      var idx = 0
      val iter2 = map.int2IntEntrySet().fastIterator()
      while (iter2.hasNext) {
        val entry = iter2.next()
        arr(idx) = (entry.getIntKey, entry.getIntValue)
        idx += 1
      }

      arr.iterator
    }.reduceByKey(_ + _, numPartitions)
  }

  def outDegrees(edges: RDD[(Int, Int)], numPartitions: Int): RDD[(Int, Int)] = {
    edges.mapPartitions { case iter =>
      val map = new Int2IntOpenHashMap()
      iter.foreach { case (src, _) =>
        map.addTo(src, 1)
      }

      val arr = new Array[(Int, Int)](map.size())
      var idx = 0
      val iter2 = map.int2IntEntrySet().fastIterator()
      while (iter2.hasNext) {
        val entry = iter2.next()
        arr(idx) = (entry.getIntKey, entry.getIntValue)
        idx += 1
      }

      arr.iterator
    }.reduceByKey(_ + _, numPartitions)
  }

}
