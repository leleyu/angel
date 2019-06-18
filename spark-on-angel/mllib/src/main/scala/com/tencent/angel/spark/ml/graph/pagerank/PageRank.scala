package com.tencent.angel.spark.ml.graph.pagerank

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntLongVector, LongIntVector}
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.NodeIndexer
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, FloatParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class PageRank(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputPageRankCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize {


  final val tol = new DoubleParam(this, "tol", "tol")
  final val resetProb = new FloatParam(this, "resetProb", "resetProb")

  final def setTol(error: Double): this.type = set(tol, error)
  final def setResetProb(prob: Float): this.type = set(resetProb, prob)

  setDefault(tol, 0.001)
  setDefault(resetProb, 0.15f)

  def this() = this(Identifiable.randomUID("PageRank"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val rawEdges = dataset.select(${srcNodeIdCol}, ${dstNodeIdCol}).rdd.flatMap {
      case row =>
        val src = row.getLong(0)
        val dst = row.getLong(1)
        if (src != dst)
          Iterator.single((src, dst))
        else
          Iterator.empty
    }

    val nodes = rawEdges.mapPartitions {
      case iter =>
        val set = new LongOpenHashSet()
        iter.foreach { case (src, dst) =>
          set.add(src)
          set.add(dst)
        }
        set.toLongArray().iterator
    }.distinct(${partitionNum})

    val indexer = new NodeIndexer()
    indexer.train(${psPartitionNum}, nodes)

    // encode
    val edges: RDD[(Int, Int)] = indexer.encode(rawEdges, $(batchSize)) {
      case (arr, ps) =>
        val keys = arr.flatMap(t => Array(t._1, t._2)).distinct
        val map = ps.pull(keys.clone()).asInstanceOf[LongIntVector]
        arr.map { case (src, dst) =>
          val intSrc = map.get(src)
          val intDst = map.get(dst)
          (intSrc, intDst)
        }.iterator
    }

    edges.persist($(storageLevel))

    val numNodes = indexer.getNumNodes
    println(s"numNodes=$numNodes")
    val model = PageRankPSModel.fromMaxId(numNodes)


    def addOutDegs(iter: Iterator[(Int, Int)]): Unit = {
      val update = VFactory.sparseDoubleVector(model.dim)
      val map = new Int2IntOpenHashMap()
      iter.foreach { case (src, _) =>
        map.addTo(src, 1)
      }

      val it = map.int2IntEntrySet().fastIterator()
      while (it.hasNext) {
        val entry = it.next()
        update.set(entry.getIntKey, entry.getIntValue)
      }

      model.addNumOutLink(update)
    }

    // init out degrees on parameter server
    edges.foreachPartition(addOutDegs)

    val graph = edges.map(sd => (sd._2, sd._1)).groupByKey(${partitionNum})
      .mapPartitions(iter => Iterator.single(PageRankGraphPartition.apply(iter)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    indexer.destroyEncoder()
    edges.unpersist(false)

    model.init($(resetProb))

    var numActives = model.numActive()

    println(s"numActives=$numActives")

    var numModified = 0
    do {
      numModified = graph.map(_.process(model, $(resetProb), ${tol}, numNodes)).reduce(_ + _)
      numActives = model.numActive()
      println(s"numModified=$numModified numActives=$numActives")
    } while (numModified > 0)

    val retRDD = graph.map(_.save(model))

    // decode
    val result = indexer.decodePartition[(Array[Int], Array[Double]), (Long, Double)](retRDD) { ps =>
      iter =>
        if (iter.nonEmpty) {
          val (node, rank) = iter.next()
          val intKeys = ps.pull(node.clone()).asInstanceOf[IntLongVector].get(node)
          intKeys.zip(rank).toIterator
        } else {
          Iterator.empty
        }
    }.map { case (node, rank) =>
      Row.fromSeq(Seq[Any](node, rank))
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(result, outputSchema)
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputPageRankCol)}", DoubleType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
