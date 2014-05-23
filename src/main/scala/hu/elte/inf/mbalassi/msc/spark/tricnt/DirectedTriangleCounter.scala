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
    val graph = inputData.map(line => line.split(' ').map(vertex => vertex.toInt).toList)
                        .map(line => (line.head, line.tail)).cache

    val bidir = graph.map{case (vertex, links) => (links.filter(_ > vertex), vertex)}
    					.flatMap{case (targets, vertex) => targets zip List.fill(targets.length)(vertex)}
    val triCan = (graph.map{case (vertex, links) =>(vertex, links.filter(vertex > _))} join bidir)
                        .flatMap{case (vertex, (links, target)) => links zip List.fill(links.length)(target)}
    val triCnt = (graph join triCan).filter{case (vertex, (links, target)) => links.contains(target)}.count 
    println("TRI_CNT: %d".format(triCnt))
  }
}
