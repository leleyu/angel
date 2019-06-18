package com.tencent.angel.spark.examples.basic

import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

object GraphXPR {

  def main(args: Array[String]): Unit = {
    val input = "data/bc/edge"
    val output = "model/graphx/pagerank"

    start()

    val edges = GraphIO.load(input, isWeighted = false)
      .select("src", "dst").rdd.flatMap {
      case row =>
        val src = row.getLong(0)
        val dst = row.getLong(1)
        if (src != dst)
          Iterator.single(Edge(src, dst, 1))
        else
          Iterator.empty
    }


    val graph = Graph.fromEdges(edges, 1)
    val pr = graph.pageRank(0.000001, 0.15)
    val ranks = pr.vertices.map(f => s"${f._1} ${f._2}")
    ranks.saveAsTextFile(output)
  }


  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("GraphXPageRank")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    sc.setCheckpointDir("cp")
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
