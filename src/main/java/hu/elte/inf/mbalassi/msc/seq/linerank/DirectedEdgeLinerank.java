package hu.elte.inf.mbalassi.msc.seq.linerank;

import java.util.Arrays;

/**
 * This class implements the calculation of the PageRank scores of each node in the line graph of
 * a directed graph. The resulting edge scores are the ones aggregated when calculating LineRank
 * values for nodes.
 * 
 */
public class DirectedEdgeLinerank {
    
    private static final double EPS = 0.000000001;

    /**
     * Calculate the edge scores used in LineRank for the given graph.
     * 
     * @param graph the graph
     * @return the score of each edge in the corresponding position of the adjacency list
     */
    public static double[][] of(int[][] graph) {
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

        double[][] res = new double[n][];
        int k = 0;
        for (int i = 0; i < n; ++i) {
            res[i] = new double[graph[i].length];
            for (int j = 0; j < graph[i].length; ++j) {
                res[i][j] = vector[k++];
            }
        }
        return res;
    }

}
