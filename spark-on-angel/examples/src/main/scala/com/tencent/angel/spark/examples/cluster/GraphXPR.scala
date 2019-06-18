package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

object GraphXPR {

  def main(args: Array[String]): Unit = {
    val param = ArgsUtil.parse(args)
    val mode = param.getOrElse("mode", "yarn-cluster")
    val input = param.getOrElse("input", "")
    val output = param.getOrElse("output", null)

    val sc = start(mode)

    val edges = GraphIO.load(input, isWeighted = false, sep = "\t")
      .select("src", "dst").rdd.flatMap {
      case row =>
        val src = row.getLong(0)
        val dst = row.getLong(1)
        if (src != dst)
          Iterator.single(Edge(src, dst, 1))
        else
          Iterator.empty
    }.repartition(500)


    val graph = Graph.fromEdges(edges, 1)
    val pr = graph.pageRank(0.001, 0.15)
    val ranks = pr.vertices.map(f => s"${f._1} ${f._2}")
    ranks.saveAsTextFile(output)
  }


  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("GraphXPageRank")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
