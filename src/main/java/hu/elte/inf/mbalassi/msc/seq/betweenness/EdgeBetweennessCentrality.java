package hu.elte.inf.mbalassi.msc.seq.betweenness;

import java.util.Arrays;

/**
 * This class implements the betweenness centrality calculation for the edges of a graph. 
 * 
 */
public class EdgeBetweennessCentrality {
    
    private boolean undirected = false;
    
    private EdgeBetweennessCentrality(boolean undirected) {
        this.undirected = undirected;
    }
    
    /**
     * Calculate the edge betweenness centrality of the given directed graph. For undirected graphs,
     * use {@link #ofUndirected(int[][])}.
     * 
     * @param graph the graph
     * @return the score of each edge in the corresponding position of the adjacency list
     */
    public static double[][] of(int[][] graph) {
        EdgeBetweennessCentrality calc = new EdgeBetweennessCentrality(false);
        calc.calculate(graph);
        return calc.result;
    }
    
    /**
     * Calculate the edge betweenness centrality of the given undirected graph. In the input
     * adjacency list, each edge should be contained twice: once in both directions. Similar to
     * {@link #of(int[][])}, but score of each edge in one direction is added to its score in
     * the other direction.
     * 
     * @param graph the graph
     * @return the score of each edge in the corresponding position of the adjacency list
     */
    public static double[][] ofUndirected(int[][] graph) {
        EdgeBetweennessCentrality calc = new EdgeBetweennessCentrality(true);
        calc.calculate(graph);
        return calc.result;
    }
    
    private double[][] result;
    
    private double[][] calculate(int[][] graph) {
        int n = graph.length;

        int[][] edgeId = null;
        int m = 0;
        if (undirected) {
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
            edgeId = new int[n][];
            m = 0;
            for (int i = 0; i < n; ++i) {
                edgeId[i] = new int[graph[i].length];
            }
            int[] counter = new int[n];
            for (int node = 0; node < n; ++node) {
                for (; counter[node] < graph[node].length; ++counter[node]) {
                    edgeId[node][counter[node]] = m;
                    int otherNode = graph[node][counter[node]];
                    edgeId[otherNode][counter[otherNode]] = m;
                    ++counter[otherNode];
                    ++m;
                }
            }
        }
        
        result = new double[n][];
        for (int i = 0; i < n; ++i) {
            result[i] = new double[graph[i].length];
        }
        for (int i = 0; i < n; ++i) {
            calculateFromOneNode(graph, i);
        }
        
        if (undirected) {
            double[] edgeScore = new double[m];
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < graph[i].length; ++j) {
                    edgeScore[edgeId[i][j]] += result[i][j];
                }
            }
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < graph[i].length; ++j) {
                    result[i][j] = edgeScore[edgeId[i][j]];
                }
            }
        }
        
        return result;
    }

    private void calculateFromOneNode(int[][] graph, int s) {
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
            for (int i = 0; i < graph[u].length; ++i) {
                int v = graph[u][i];
                if (dist[v] == dist[u] + 1) {
                    double sc = (res[v] + 1.0) * (pathsTo[u] / pathsTo[v]);
                    result[u][i] += sc;
                    res[u] += sc;
                }
            }
        }
    }

}
