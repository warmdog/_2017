import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = new SparkContext("local","wordcount",conf)
    val list = List('x','a')
    val rdd1=sc.parallelize(List(('a',2),('b',4),('c',6),('d',9)))
    val rdd2=sc.parallelize(List(('c',6),('c',7),('d',8),('e',10)))
    val rdd4=rdd1.map(x =>x._1)
    val rdd5 = rdd1.map(x =>{
      if(list.contains(x._1)){
        x._1
      }
    })
    rdd5.foreach(println(_))
   // val rdd3 = rdd4.union(rdd5).distinct().count()
    var co =0;
    //println(rdd4.union(rdd5).glom().collect().distinct.size)
   // val  re =rdd1.intersection(rdd2).count()
   // println(rdd3)
  }

}
