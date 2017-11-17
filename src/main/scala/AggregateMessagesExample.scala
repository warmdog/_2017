/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
//package org.apache.spark.examples.graphx

// $example on$
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, TripletFields, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example use the [`aggregateMessages`][Graph.aggregateMessages] operator to
  * compute the average age of the more senior followers of each user
  * Run with
  * {{{
  * bin/run-example graphx.AggregateMessagesExample
  * }}}
  */
object AggregateMessagesExample {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
        .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getLogger("org").setLevel(Level.WARN)
    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    // Compute the number of older followers and their total age
    graph.edges.foreach(println(_))
    //graph.edges.foreach(println(_))
    //graph.filter()
    // 计算 每个点 的老用户 数量和 年龄 生成新的VertexRDD
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          //triplet.
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      //Add counter and age
      //graph.vertices.mapValues()
      (a, b) => (a._1 + b._1, a._2 + b._2), // Reduce Function
      TripletFields.All
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$
   // println(graph.inDegrees.max().)
    // 使用pregel 快速 迭代运算
    //发现最短路径
    val sourceId:VertexId=99
   val initialGraph = graph.mapVertices((id, _) =>
     if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val value: Graph[Double, Int] = initialGraph.pregel(Double.PositiveInfinity)(
      (a, newDist, dist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        //triplet.
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    val sssp = value
    println(" Pregel: =========")
    println(sssp.vertices.collect.mkString("\n"))
    spark.stop()
  }
  def max(a:(VertexId,Int), b:(VertexId,Int)):(VertexId,Int)={
    if(a._2>b._2) a else b
  }
}
// scalastyle:on println
