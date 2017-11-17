
import scala.collection.mutable.Map
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.io.Source

object sourceFromFile {
  def main(args: Array[String]): Unit = {
    val file=Source.fromFile("d:/Users/Desktop/part-00000")
    val regex = "[0-9|,]".r
    var sum =0
    var res = Map[Long,Int]()
    for (line <- file.getLines()) {
      val tmp = (regex findAllIn line).mkString("").split(",")
      sum += tmp(1).toInt
      res += (tmp(0).toLong->tmp(1).toInt)

    }
    res.toList.sortWith(_._1 < _._1).foreach(println(_))
//    val resu =TreeMap[Long,Int]()(ord = {}) ++res
    print(sum)
//    println("xxxxxx")
//   resu.foreach(println(_))
  }
}
