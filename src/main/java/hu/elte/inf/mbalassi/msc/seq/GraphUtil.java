package hu.elte.inf.mbalassi.msc.seq;

/**
 * Utilities for directed graphs using the int[][] adjacency list representation.
 * 
 */
public class GraphUtil {

    /**
     * Calculate the transpose graph.
     * 
     * @param graph a directed graph
     * @return the transpose graph
     */
    public static int[][] transpose(int[][] graph) {
        int n = graph.length;

        int[] degree = new int[n];
        for (int[] edges : graph) {
            for (int endpoint : edges) {
                ++degree[endpoint];
            }
        }

        int[][] res = new int[n][];
        for (int i = 0; i < n; ++i) {
            res[i] = new int[degree[i]];
        }

        int[] next = new int[n];
        for (int i = 0; i < n; ++i) {
            for (int endpoint : graph[i]) {
                res[endpoint][next[endpoint]++] = i;
            }
        }

        return res;
    }

}
