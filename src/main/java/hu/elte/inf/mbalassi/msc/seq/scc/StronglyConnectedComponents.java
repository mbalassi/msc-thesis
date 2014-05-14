package hu.elte.inf.mbalassi.msc.seq.scc;

import java.util.Arrays;
import java.util.Stack;

/**
 * Class for calculating the strongly connected components of a directed graph. The path-based
 * strong component algorithm by Gabow is used, which runs in linear time.
 * 
 */
public final class StronglyConnectedComponents {
    
    /**
     * Calculate the strongly connected components of the given directed graph.
     * 
     * @param graph the graph as an adjacency list
     * @return the {@link StronglyConnectedComponents} object containing the solution
     */
    public static StronglyConnectedComponents of(int[][] graph) {
        return new StronglyConnectedComponents(graph);
    }

    // the component each node belongs to (-1 if not assigned yet)
    private int[] res;
    // the preorder number of each node (-1 if not assigned yet)
    private int[] pre;
    // counter for preorder numbers
    int C;
    // visited nodes not assigned to a SCC yet
    Stack<Integer> S;
    // visited nodes which may still belong to different SCCs
    Stack<Integer> P;
    // graph structure
    int[][] graph;
    
    /**
     * Calculate the SCCs.
     * 
     * @param graph the graph
     */
    private StronglyConnectedComponents(int[][] graph) {
        this.graph = graph;
        res = new int[graph.length];
        Arrays.fill(res, -1);
        pre = new int[graph.length];
        Arrays.fill(pre, -1);
        C = 0;
        S = new Stack<Integer>();
        P = new Stack<Integer>();
        
        for (int i = 0; i < graph.length; ++i) {
            if (pre[i] == -1) {
                dfs(i);
            }
        }

        this.graph = null;
        pre = null;
        S = null;
        P = null;
    }
    
    private void dfs(int v) {
        pre[v] = C++;
        S.push(v);
        P.push(v);
        for (int i = 0; i < graph[v].length; ++i) {
            int w = graph[v][i];
            if (pre[w] == -1) {
                dfs(w);
            } else {
                if (res[w] == -1) {
                    while (pre[P.peek()] > pre[w]) {
                        P.pop();
                    }
                }
            }
        }
        if (P.peek().compareTo(v) == 0) {
            int i = -1;
            while (i != v) {
                i = S.pop();
                res[i] = v;
            }
            P.pop();
        }
    }
    
    /**
     * Get the component of each node. The integer denoting the component is the ID of a
     * representative node in the component.
     * 
     * @return the component each node belongs to
     */
    public int[] get() {
        return res;
    }
}
