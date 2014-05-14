package hu.elte.inf.mbalassi.msc.seq.betweenness;

import java.util.Arrays;

/**
 * Calculates the betweenness centrality of each node. The number of shortest paths going through it is returned for
 * each node. If k shortest paths exist between two nodes, each path contributes only 1/k to the score of the nodes it
 * passes through. A path does not contribute to the score of its end points.
 * 
 */
public final class BetweennessCentrality {

    /**
     * Does the actual calculation.
     * 
     * @param graph the graph
     * @return the betweenness centrality of each node
     */
    public static double[] of(int[][] graph) {
        int n = graph.length;
        double[] res = new double[n];
        for (int i = 0; i < n; ++i) {
            double[] partialRes = calculateFromOneNode(graph, i);
            for (int j = 0; j < n; ++j) {
                res[j] += partialRes[j];
            }
            if (i % 10 == 9) {
                System.err.println(String.format("%d/%d BFS done", i+1, n));
            }
        }
        return res;
    }
    
    /**
     * Gives an approximate result by only considering paths from certain nodes.
     * @param graph the graph
     * @param sample for each node, whether paths starting from it should be considered
     * @return the approximate scores
     */
    public static double[] sample(int[][] graph, boolean[] sample) {
        int n = graph.length;
        if (n != sample.length) {
            throw new IllegalArgumentException("Parameters should have the same length");
        }
        double[] res = new double[n];
        for (int i = 0; i < n; ++i) {
            if (sample[i]) {
                double[] partialRes = calculateFromOneNode(graph, i);
                for (int j = 0; j < n; ++j) {
                    res[j] += partialRes[j];
                }
            }
        }
        return res;
    }

    /**
     * Calculates the contributions of the paths from one node.
     * 
     * @param graph the graph.
     * @param s the node.
     * @return the partial scores.
     */
    private static double[] calculateFromOneNode(int[][] graph, int s) {
        int n = graph.length;
        // Initialize
        int[] queue = new int[n]; // queue containing s
        queue[0] = s;
        int qf = 0; // queue front
        int qb = 1; // queue back
        int[] stack = new int[n]; // empty stack
        int sp = 0; // stack pointer
        double[] pathsTo = new double[n]; // number of shortest paths from s to each node
        pathsTo[s] = 1.0;
        int[] dist = new int[n]; // distance from s for each node
        Arrays.fill(dist, -1);
        dist[s] = 0;
        // BFS
        while (qf < qb) {
            int u = queue[qf++];
            for (int i = 0; i < graph[u].length; ++i) {
                int v = graph[u][i];
                if (dist[v] < 0) {
                    dist[v] = dist[u] + 1;
                    queue[qb++] = v;
                }
                if (dist[v] == dist[u] + 1) {
                    pathsTo[v] += pathsTo[u];
                }
            }
            stack[sp++] = u;
        }
        // Calculate scores
        double[] res = new double[n];
        while (sp > 0) {
            int u = stack[--sp];
            if (u == s) continue;
            for (int i = 0; i < graph[u].length; ++i) {
                int v = graph[u][i];
                if (dist[v] == dist[u] + 1) {
                    res[u] += (res[v] + 1.0) * (pathsTo[u] / pathsTo[v]);
                }
            }
        }
        return res;
    }

}
