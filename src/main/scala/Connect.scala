//import org.apache
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
case class Record(key:Int,value:Boolean)
object Connect {

  def main(args: Array[String]): Unit = {
    println("ffee")
    val warehouseLocation = new File("/user/hive/warehouse/").getAbsolutePath
    val a = "d:/Users/Desktop/test.txt"

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("Spark Hive").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    //支持通配符路径，支持压缩文件读取
    val text = spark.sparkContext.textFile(a)
//    val rdd1 = text.mapPartitions(x =>{
//      var result = List[Long]()
//      while(x.hasNext){
//        val y= x.next().toLong
//        result.::=(y)
//      }
//      result.iterator
//    })
    val rdd2=text.map(x =>{
      val a = x.split("\\s+")
        a(0).toLong
      })
    //rdd2.
    val count = spark.sql("select count(1)  from blacklist.black_type_list_new")
    val count1 =count.toDF()
    //count1.union(count1)
    //spark.newSession()
    // count1.intersect()
    //count.map(x => x.toString().toLong)
    Logger.getLogger("Connect.class").info(count.first())
  }

}
