package hu.elte.inf.mbalassi.msc.seq.tricnt;

public class DirectedTriangleCounter{

   private static long countDepthNInsts(int node, int goalNode, int depth,
                                        int[][] graph){
     if (depth == 0) return (node == goalNode ? 1 : 0);

     long cnt = 0;
     for (int dest = 0; dest < graph[node].length; ++dest){
         cnt += countDepthNInsts(graph[node][dest], goalNode, depth-1, graph);
     }
     return cnt;
   }
   
   public static long count(int[][] intGraph) {
       long triCnt = 0;

       int numNodes = intGraph.length;

       for(int node = 0; node < numNodes; ++node){
         triCnt += countDepthNInsts(node, node, 3, intGraph);
       }

       //every triangle is counted 3 times
       triCnt /= 3;
       
       return triCnt;
   }
}
