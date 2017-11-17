

import java.io
import java.util.Collections

import scala.io.Source
// for solving Travling salesman Problem


//index, x,y are the cordinates of Node
case class Node (index:Int,x:Int,y:Int)
case class Cache(max:Double,min:Double,average:Double)
object HdfsReader {
  var cahce = Cache(0.0,0.0,0.0)
  def main(args: Array[String]): Unit = {

    val file = Source.fromFile("d:/Users/Desktop/at48.tsp")
    var nodes = Array[Node]()
    //println(file.getLines().length)
    for(line <- file.getLines().dropWhile(_.contains("a"))){
      val part =line.split(" ")
      //List.concat(List(new Node(part(0).toInt,part(1).toInt,part(2).toInt)),nodes)
      println(part(0).toInt,part(1).toInt,part(2).toInt)
      nodes = nodes ++ Array(Node(part(0).toInt,part(1).toInt,part(2).toInt))
    }
    val tes =saAlgorithm(nodes)
    file.close()
  }
  //
  def getTwoCitiesDistance(x:Node,y:Node) : Double ={
     val xD = Math.abs(x.x-y.x)
     val yD = Math.abs(x.y-y.y)
     val res = Math.sqrt(xD*xD+yD*yD)
     res
  }

  def getAllCitiesDistance(list:Array[Node]) : Double ={
    var res = 0.0
    //res = list.map(x =>)
    for(i <-0 until (list.size-1)){
      res = res + getTwoCitiesDistance(list(i+1),list(i))
    }
    res = res + getTwoCitiesDistance(list(list.size-1),list(0))
    res
  }

  def getRandomPath(list: Array[Node]): Array[Node] = {


    val leftNode  = util.Random.shuffle(list.toList)
    val newNodes = Array()++leftNode
    newNodes
  }
  def getNeighbor(list:Array[Node]):Array[Node] ={
    val node =list(0)
    var a = (new util.Random).nextInt(list.length)
    var b = (new util.Random).nextInt(list.length)
//    if(a==0){
//      a +=1
//    }
//    if(b==0){
//      b +=1
//    }
    val change = list(a)
    list(a) = list(b)
    list(b) = change
    list

  }
  //SA algorithm
  def saAlgorithm(nodes:Array[Node]): Unit={
    //temperature
    var max =0.0
    var min =Double.MaxValue
    var average = 0.0
   for(i <-1 to 10){
     var tem =100.0;
     val coolingRate = 0.03
     var path = getRandomPath(nodes)
     var best = path
     while(tem>1.0){
       val newPath = getNeighbor(path)
       val currentDistance = getAllCitiesDistance(path)
       val newDistance = getAllCitiesDistance(newPath)
       if(acceptanceProbability(currentDistance,newDistance,tem)>math.random){
         path = newPath
       }
       if(currentDistance<getAllCitiesDistance(best)){
         best = path
       }
       tem *= 1-coolingRate
     }
     val bestDistance =getAllCitiesDistance(best)
     max = math.max(max,bestDistance)
     min = math.min(min,bestDistance)
     average += bestDistance
     println(s"'${i}'：'${bestDistance.toInt}' :'${best(0)}'")
   }
    println(max.toInt)
    println(min.toInt)
    println((average/10).toInt)
  }
  def acceptanceProbability(energy:Double,newEnergy:Double,temperature:Double):Double={
    if(newEnergy<energy){
      1.0
    }else{
      Math.exp((energy-newEnergy)/temperature)
    }
  }

}
