package com.tencent.angel.spark.ml.graph.pagerank

import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils


private[pagerank] class PageRankPSModel(val matrix: PSMatrix) extends Serializable {

  val dim: Int = matrix.columns.toInt
  private val ranks: PSVector = new PSVectorImpl(matrix.id, 0, dim, matrix.rowType)
  private val outDegs: PSVector = new PSVectorImpl(matrix.id, 1, dim, matrix.rowType)
  private val flags: PSVector = new PSVectorImpl(matrix.id, 2, dim, matrix.rowType)

  def init(value: Double): Unit = {
    // need not to init outDegs since we have initialized outDegs with out-degrees
//    VectorUtils.randomUniform(ranks, 0, 1.0)
    ranks.fill(value)
    flags.fill(1.0)
  }

  def updateNumOutLinks(vector: IntDoubleVector): Unit = {
    outDegs.update(vector)
  }

  def addNumOutLink(vector: IntDoubleVector): Unit = {
    outDegs.increment(vector)
  }

  def updateRanksAndFlags(updateRanks: Vector, updateFlags: Vector): Unit = {
    matrix.update(Array(0, 2), Array(updateRanks, updateFlags))
  }

  def pull(nodes: Array[Int]): (IntDoubleVector, IntDoubleVector) = {
    val vectors = matrix.pull(Array(0, 1), nodes)
    (vectors(0).asInstanceOf[IntDoubleVector],
      vectors(1).asInstanceOf[IntDoubleVector])
  }

  def pullFlags(nodes: Array[Int]): IntDoubleVector = {
    flags.pull(nodes).asInstanceOf[IntDoubleVector]
  }

  def updateActiveFlag(vector: IntDoubleVector): Unit = {
    flags.update(vector)
  }

  def numActive(): Int =
    VectorUtils.sum(flags).toInt

}


private[pagerank] object PageRankPSModel {
  def fromMaxId(maxId: Int): PageRankPSModel = {
    // the first row stores the rank value for each node
    // the second row stores the number of out-links for each node
    // the third row stores the update rank value for the last iteration
    val matrix = PSMatrix.dense(3, maxId, rowType = RowType.T_DOUBLE_DENSE)
    new PageRankPSModel(matrix)
  }
}
