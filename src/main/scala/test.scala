import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.rdd.RDD

//import com.sun.javafx.scene.paint.GradientUtils.Parser

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoader
object test {
  val filePath = "d:/Users/Desktop/documents/0*"
  def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val a = "d:/Users/Desktop/test.txt"
        val conf = new SparkConf()

        val sc = new SparkContext("local","wordcount",conf)
            sc.defaultMinPartitions
        val line = sc.textFile(a)

        val count = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey((x,y)=>x+y).count()
        //println(count)
        //count.saveAsTextFile("/test")
        println(count)
        val res = line.coalesce(2,shuffle = true).flatMap(_.split(" ")).map(x =>{
              val item = x.split("\\s+")
              (item(0),1)
        }).reduceByKey((x,y)=>x+y)//.sortBy(_._2,false).coalesce(2,shuffle = true)
       println(res.getNumPartitions)
       res.foreachPartition(it => {
          println(it.mkString(" "))
       })
       // println(tes)
        //Graph.fromEdges()
    // Load the edges as a graph
        val graph = GraphLoader.edgeListFile(sc, "D:/workTool/spark-2.2.0-bin-hadoop2.7/data/graphx/followers.txt")
    //
        val lp = LabelPropagation.run(graph,10)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices
        //
        val cc = graph.connectedComponents(100).vertices
        //cc.foreach(println(_))
        //ranks.foreach(println(_))
        //println( ranks.max())
        // Join the ranks with the usernames
        val users = sc.textFile("D:/workTool/spark-2.2.0-bin-hadoop2.7/data/graphx/users.txt").map { line =>
              val fields = line.split(",")
              println("TEST")
              //println(fields.mkString(".."))
              (fields(0).toLong, fields.mkString(".."))
        }
        val ranksByUsername = users.leftOuterJoin(ranks).map {
              case (id, (username, rank)) => (id,username, rank)
        }
        val ccByUsername = users.join(cc).map {
              case (id, (username, cc)) => (username, cc)
        }
        // Print the result
        println("connected components")
        println(ccByUsername.collect().mkString("\n"))
        // Print the result
        println(ranksByUsername.collect().mkString("\n"))
        Logger.getLogger(test.getClass).info(s"xxxxxxxxxxxxx'${count}'")
        loadData(sc)
        sc.stop()
  }
      def  loadData(sc :SparkContext) :RDD[String] ={
            val pps =new Properties()
            pps.load(new FileInputStream("D:/workTool/_2017/src/main/resources/log4j2.properties"))
            val alldata = sc.textFile(pps.getProperty("filePath"))
            println(s"sssssss'${alldata.count()}'")
            val count = alldata.map(x =>1).reduce((x,y) =>x+y)

            alldata
      }
}


