package hu.elte.inf.elte.mbalassi.msc.spark.tricnt;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object DirectedTriangleCounter {
  def main(args: Array[String]) {
    
    val sparkHome = args(0)
    val input = args(1)
    
    val sc = new SparkContext("local", "DirectedTriangleCounter", sparkHome,
      List("target/scala-2.10/triangle-counter_2.10-1.0.jar"))

    val inputData = sc.textFile(input, 2)
    val graph = inputData.map(line => line.split(' ').map(x => x.toInt).toList)
                        .map(line => (line.head, line.tail)).cache

    val bidir = graph.map(pair => (pair._2.filter(_ > pair._1), pair._1))
    val bidirOut = bidir.flatMap{pair => pair._1 zip 
                        List.fill(pair._1.length)(pair._2)}
    val triCan = (graph.map(pair => (pair._1, pair._2.filter(pair._1 > _))) join bidirOut)
                        .flatMap{pair => pair._2._1 zip List.fill(pair._2._1.length)(pair._2._2)}
    val triCnt = (graph join triCan).filter(pair => pair._2._1.contains(pair._2._2)).count
    println("TRI_CNT: %d".format(triCnt))
  }
}
