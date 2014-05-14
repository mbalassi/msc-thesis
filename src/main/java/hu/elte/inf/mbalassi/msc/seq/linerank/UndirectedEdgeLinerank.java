package hu.elte.inf.mbalassi.msc.seq.linerank;

import java.util.Arrays;

/**
 * This class implements the calculation of the PageRank scores of each node in the line graph of
 * an undirected graph. The resulting edge scores are the ones aggregated when calculating LineRank
 * values for nodes.
 * 
 */
public class UndirectedEdgeLinerank {

    private static final double EPS = 0.000000001;

    /**
     * Calculate the edge scores used in LineRank for the given undirected graph. In the input
     * adjacency list, each edge should be contained twice: once in both directions.
     * 
     * @param graph the graph
     * @return the score of each edge in the corresponding position of the adjacency list. The
     * score is the same for the two directions of the same edge.
     */
    public static double[][] of(int[][] graph) {
        // Initialize
        int n = graph.length;
        // Sort if necessary
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < graph[i].length - 1; ++j) {
                if (graph[i][j] > graph[i][j+1]) {
                    Arrays.sort(graph[i]);
                    break;
                }
            }
        }
        
        // Connect two directions of the same edge
        int[][] edgeId = new int[n][];
        int m = 0;
        {
            for (int i = 0; i < n; ++i) {
                edgeId[i] = new int[graph[i].length];
            }
            int[] counter = new int[n];
            for (int node = 0; node < n; ++node) {
                while (counter[node] < graph[node].length) {
                    edgeId[node][counter[node]] = m;
                    int otherNode = graph[node][counter[node]];
                    ++counter[node];
                    edgeId[otherNode][counter[otherNode]] = m;
                    if (node != graph[otherNode][counter[otherNode]]) {
                        throw new IllegalArgumentException("Graph should contain each edge twice:" +
                        		" once for each direction");
                    }
                    ++counter[otherNode];
                    ++m;
                }
            }
        }
        double[] edgeDegree = new double[m];
        for (int i = 0; i < n; ++i) {
            double degree = (double)(graph[i].length - 1);
            for (int j = 0; j < graph[i].length; ++j) {
                edgeDegree[edgeId[i][j]] += degree;
            }
        }

        // Initialize iterative computation
        double startAtNodeProb = 1.0 / (double)m;
        double restartProb = 0.15;
        double[] score = new double[m];
        Arrays.fill(score, startAtNodeProb);
        boolean moreIterations = true;
        double[] tmpScore = new double[m];
        double[] tmpNodeScore = new double[n];
        // Iterate
        while (moreIterations) {
            Arrays.fill(tmpScore, 0.0);
            Arrays.fill(tmpNodeScore, 0.0);
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < graph[i].length; ++j) {
                    int edge = edgeId[i][j];
                    if (edgeDegree[edge] > 0) {
                        double chanceEach = score[edge] / edgeDegree[edge];
                        tmpScore[edge] -= chanceEach;
                        tmpNodeScore[graph[i][j]] += chanceEach;
                    } else {
                        // if we can't go anywhere, stay at this edge
                        tmpScore[edge] += score[edge] / 2.0;
                    }
                }
            }
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < graph[i].length; ++j) {
                    tmpScore[edgeId[i][j]] += tmpNodeScore[i];
                }
            }
            for (int i = 0; i < m; ++i) {
                tmpScore[i] = restartProb * startAtNodeProb + (1.0 - restartProb) * tmpScore[i];
            }
            
            // Check convergence
            moreIterations = false;
            for (int i = 0; i < m; ++i) {
                if (Math.abs(tmpScore[i] - score[i]) > EPS) {
                    moreIterations = true;
                    break;
                }
            }
            double[] tmp = score;
            score = tmpScore;
            tmpScore = tmp;
        }

        double[][] res = new double[n][];
        for (int i = 0; i < n; ++i) {
            res[i] = new double[graph[i].length];
            for (int j = 0; j < graph[i].length; ++j) {
                res[i][j] = score[edgeId[i][j]];
            }
        }
        return res;
    }
    
}
