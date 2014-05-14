package hu.elte.inf.mbalassi.msc.seq.linerank;

import java.util.Arrays;

/**
 * This class implements the calculation of LineRank centralities for graphs.
 * 
 */
public class Linerank {
    
    private static final double EPS = 0.000000001;

    /**
     * Calculate the LineRank score of each node in a directed graph. For undirected graphs, use
     * {@link #ofUndirected(int[][])}.
     * 
     * @param graph the graph
     * @return the LineRank score of each node
     */
    public static double[] of(int[][] graph) {
        // Initialize
        int n = graph.length;
        int m = 0;
        for (int i = 0; i < n; ++i) {
            m += graph[i].length;
        }
        // Build incidence matrices S and T
        int[] s = new int[m]; // index of cell set to 1 for each row in T(G)
        int[] t = new int[m]; // index of cell set to 1 for each row in S(G)
        int c = 0;
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < graph[i].length; ++j) {
                s[c] = i;
                t[c] = graph[i][j];
                c++;
            }
        }

        // Initialize iterative computation
        double startAtNodeProb = 1.0 / (double)m;
        double restartProb = 0.15;
        double[] vector = new double[m];
        Arrays.fill(vector, startAtNodeProb);
        boolean moreIterations = true;
        double[] tmpNodeVector = new double[n];
        double[] tmpVector = new double[m];
        // Iterate
        while (moreIterations) {
            Arrays.fill(tmpNodeVector, 0.0);
            for (int i = 0; i < m; ++i) {
                tmpNodeVector[t[i]] += vector[i];
            }
            for (int i = 0; i < n; ++i) {
                if (graph[i].length > 0) {
                    tmpNodeVector[i] /= (double)graph[i].length;
                    tmpNodeVector[i] = startAtNodeProb*restartProb + tmpNodeVector[i]*(1.0-restartProb);
                }
            }
            for (int i = 0; i < m; ++i) {
                tmpVector[i] = tmpNodeVector[s[i]];
            }
            // Check convergence
            moreIterations = false;
            for (int i = 0; i < m; ++i) {
                if (Math.abs(tmpVector[i] - vector[i]) > EPS) {
                    moreIterations = true;
                    break;
                }
            }
            double[] tmp = vector;
            vector = tmpVector;
            tmpVector = tmp;
        }
        
        double[] res = new double[n];
        for (int i = 0; i < m; ++i) {
            res[t[i]] += vector[i];
            res[s[i]] += vector[i];
        }
        return res;
    }
    
    /**
     * Calculate the LineRank score of each node in an undirected graph. In the input adjacency
     * list, each edge should be contained twice: once in both directions.
     * 
     * @param graph the graph
     * @return the LineRank score of each node
     */
    public static double[] ofUndirected(int[][] graph) {
        double[][] edgePageRank = UndirectedEdgeLinerank.of(graph);
        double[] res = new double[graph.length];
        for (int i = 0; i < edgePageRank.length; ++i) {
            for (int j = 0; j < edgePageRank[i].length; ++j) {
                res[i] += edgePageRank[i][j];
            }
        }
        return res;
    }
}
