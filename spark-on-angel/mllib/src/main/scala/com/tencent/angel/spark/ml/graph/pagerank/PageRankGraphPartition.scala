package com.tencent.angel.spark.ml.graph.pagerank

import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector}
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, IntArrayList, IntOpenHashSet}

class PageRankGraphPartition(keys: Array[Int],
                             indptr: Array[Int],
                             inNodes: Array[Int]) extends Serializable {

  assert(keys.length == indptr.length - 1)

  def activeKeys(model: PageRankPSModel): (Array[Int], Array[Int]) = {
    val flags = model.pullFlags(keys.clone())
    val actives = new IntArrayList()
    var i = 0
    val set = new IntOpenHashSet()
    while (i < keys.length) {
      if (flags.get(keys(i)) > 0) {
        set.add(keys(i))
        actives.add(i)
        var j = indptr(i)
        while (j < indptr(i + 1)) {
          set.add(inNodes(j))
          j += 1
        }
      }
      i += 1
    }

    (actives.toIntArray(), set.toIntArray())
  }


  def process(model: PageRankPSModel, q: Float, threshold: Double, N: Int): Int = {
    val (activeIndex, indices) = activeKeys(model)
    val (ranks, outDegs) = model.pull(indices)

    // pageranks
    val (outRanks, outFlags, numModified) = pagerank(activeIndex, ranks, outDegs, q, threshold, N)
    model.updateRanksAndFlags(outRanks, outFlags)
    numModified
  }

  def pagerank(activeIndex: Array[Int],
               ranks: IntDoubleVector,
               outDegs: IntDoubleVector,
               q: Float,
               threshold: Double,
               N: Int): (IntDoubleVector, IntDoubleVector, Int) = {

    var numModified = 0
    val outRanks = ranks.emptyLike()
    val outFlags = ranks.emptyLike()

    for (idx <- activeIndex) {
      var j = indptr(idx)
      var sum = 0.0
      while (j < indptr(idx + 1)) {
        val inNode = inNodes(j)
        val pj = ranks.get(inNode)
        val outDegree = outDegs.get(inNode)
        sum += pj / outDegree
        j += 1
      }

      val pr = q + sum * (1 - q)
      if (math.abs(pr - ranks.get(keys(idx))) >= threshold) {
        numModified += 1
        outRanks.set(keys(idx), pr)
      } else {
        outFlags.set(keys(idx), 0)
      }
    }

    (outRanks, outFlags, numModified)
  }

  def save(model: PageRankPSModel): (Array[Int], Array[Double]) = {
    val (ranks, _) = model.pull(keys.clone())
    (keys, ranks.get(keys))
  }

}

object PageRankGraphPartition {

  /**
    * build PageRankPartition
    * @param iter, iterator for (dst, Seq[src]) list
    * @return PageRankGraphPartition
    */
  def apply(iter: Iterator[(Int, Iterable[Int])]): PageRankGraphPartition = {
    val indptr = new IntArrayList()
    val inNodes = new IntArrayList()
    val keys = new IntArrayList()
    var idx = 0
    indptr.add(0)
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, ins) = (entry._1, entry._2)
      ins.foreach(n => inNodes.add(n))
      indptr.add(inNodes.size())
      keys.add(node)
      idx += 1
    }

    new PageRankGraphPartition(keys.toIntArray(), indptr.toIntArray(),
      inNodes.toIntArray())
  }
}
