package hu.elte.inf.mbalassi.msc.seq;

import hu.elte.inf.mbalassi.msc.seq.betweenness.BetweennessCentrality;
import hu.elte.inf.mbalassi.msc.seq.betweenness.EdgeBetweennessCentrality;
import hu.elte.inf.mbalassi.msc.seq.linerank.DirectedEdgeLinerank;
import hu.elte.inf.mbalassi.msc.seq.linerank.Linerank;
import hu.elte.inf.mbalassi.msc.seq.linerank.UndirectedEdgeLinerank;
import hu.elte.inf.mbalassi.msc.seq.scc.StronglyConnectedComponents;
import hu.elte.inf.mbalassi.msc.seq.tricnt.DirectedTriangleCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.NotImplementedException;

/**
 * Command line interface for the sequential algorithms.
 * 
 */
public class CommandLine {

    /**
     * Available algorithms.
     */
    private enum Algorithm {
        BETWEENNESS("betweenness", "Calculate the betweenness centrality for each node," +
        		" works for both directed and undirected graphs.\n\t\tOptional arguments:" +
        		"\n\t\t- ratio of considered paths, 1.0 (default) for an exact solution," +
        		" less for an approximate solution\n\t\t- random seed"),
        EDGE_BETWEENNESS("edge-betweenness", "Calculate the betweenness centrality for each edge" +
        		" of a directed graph"),
        UNDIRECTED_EDGE_BETWEENNESS("uedge-betweenness", "Calculate the betweenness centrality for each edge" +
        		" of an undirected graph"),
        LINERANK("linerank", "Calculate the linerank score for each node of a directed graph"),
        EDGE_LINERANK("edge-linerank", "Calculate the linerank score for each edge of a directed graph"),
        UNDIRECTED_LINERANK("ulinerank", "Calculate the linerank score for each node of an undirected graph"),
        UNDIRECTED_EDGE_LINERANK("uedge-linerank", "Calculate the linerank score for each edge of an undirected graph"),
        SCC("scc", "Find the strongly connected components of a directed graph" +
        		"\n\t\tOutput consists of representative node - component size pairs"),
        TRIANGLE("triangles", "Count the number of triangles in a directed graph");
        
        private String handle;
        private String description;
        
        // map from handles to the enum values
        static private Map<String, Algorithm> handleToAlg;
        
        static {
            handleToAlg = new HashMap<String, Algorithm>();
            for (Algorithm alg : Algorithm.values()) {
                handleToAlg.put(alg.getName(), alg);
            }
        }
        
        /**
         * Get the algorithm corresponding to the given handle.
         * 
         * @param handle the handle of an algorithm
         * @return the enum object for the corresponding algorithm
         */
        public static Algorithm fromName(String handle) {
            Algorithm alg = handleToAlg.get(handle);
            if (alg == null) {
                throw new IllegalArgumentException("Unknown algorithm: " + handle);
            }
            return alg;
        }
        
        private Algorithm(String handle, String description) {
            this.handle = handle;
            this.description = description;
        }
        
        public String getName() {
            return handle;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Print the values associated with each node of the graph.
     * 
     * @param graph the graph
     * @param values the value for each node
     */
    private static void printNodeValues(GraphReader graph, double[] values) {
        for (int i = 0; i < values.length; ++i) {
            System.out.println(Integer.toString(graph.originalIdOf(i)) + "\t" + Double.toString(values[i]));
        }
    }
    
    /**
     * Print the values associated with each edge of the graph.
     * 
     * @param graph the graph
     * @param values value for each edge in the corresponding position in the adjacency list
     */
    private static void printEdgeValues(GraphReader graph, double[][] values) {
        int[][] graphArray = graph.getGraph();
        for (int i = 0; i < values.length; ++i) {
            for (int j = 0; j < values[i].length; ++j) {
                System.out.println(Integer.toString(graph.originalIdOf(i))
                        + "\t" + Integer.toString(graph.originalIdOf(graphArray[i][j]))
                        + "\t" + Double.toString(values[i][j]));
            }
        }
    }
    
    /**
     * Print the values associated with each edge of the graph. From opposite edge, only one
     * will be printed.
     * 
     * @param graph the graph
     * @param values value for each edge in the corresponding position in the adjacency list
     */
    private static void printUndirectedEdgeValues(GraphReader graph, double[][] values) {
        int[][] graphArray = graph.getGraph();
        for (int i = 0; i < values.length; ++i) {
            int origNode1 = graph.originalIdOf(i);
            for (int j = 0; j < values[i].length; ++j) {
                int origNode2 = graph.originalIdOf(graphArray[i][j]);
                if (origNode1 <= origNode2) {
                    System.out.println(Integer.toString(origNode1)
                            + "\t" + Integer.toString(origNode2)
                            + "\t" + Double.toString(values[i][j]));
                }
            }
        }
    }
    
    /**
     * Entry point.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: <program> <algorithm> <input file>" +
            		" [<algorithm-dependent extra arguments>]");
            System.out.println("Available algorithms:");
            for (Algorithm alg : Algorithm.values()) {
                System.out.println("\t" + alg.getName() + " - " + alg.getDescription());
            }
            System.out.println("The input file should contain a graph, with one line describing " +
            		"each vertex. A vertex description consists of integers: the first one " +
            		"is the ID of the vertex, the rest are the IDs of the endpoints of arcs " +
            		"starting at the vertex. In case of an undirected graph, each edge should " +
            		"be given as two arcs.");
            return;
        }
        
        Algorithm alg = Algorithm.fromName(args[0]);
        GraphReader graph = new GraphReader(args[1]);
        int[][] graphArray = graph.getGraph();

        switch (alg) {
            case BETWEENNESS:
                // Create sample
                double sampleRatio = args.length < 3 ? 1.0 : Double.parseDouble(args[2]);
                if (sampleRatio <= 0.0) {
                    throw new IllegalArgumentException("BFS chance should be positive!");
                }
                Random random = args.length < 4 ? new Random() : new Random(Long.parseLong(args[2]));
                int n = graphArray.length;
                boolean[] sample = new boolean[n];
                for (int i = 0; i < n; ++i) {
                    sample[i] = random.nextDouble() < sampleRatio;
                }
                
                // Calculate betweenness centrality
                double[] betw = BetweennessCentrality.sample(graphArray, sample);
                if (sampleRatio < 1.0) {
                    for (int i = 0; i < n; ++i) {
                        betw[i] /= sampleRatio;
                    }
                }
                
                printNodeValues(graph, betw);
                break;
            case EDGE_BETWEENNESS:
                printEdgeValues(graph, EdgeBetweennessCentrality.of(graphArray));
                break;
            case EDGE_LINERANK:
                printEdgeValues(graph, DirectedEdgeLinerank.of(graphArray));
                break;
            case LINERANK:
                printNodeValues(graph, Linerank.of(graphArray));
                break;
            case SCC:
                int[] comp = StronglyConnectedComponents.of(graph.getGraph()).get();
                for (int i = 0; i < comp.length; ++i) {
                    if (comp[i] == i) {
                        comp[i] = -1;
                    }
                }
                for (int i = 0; i < comp.length; ++i) {
                    if (comp[i] >= 0) {
                        comp[comp[i]]--;
                        comp[i] = 0;
                    }
                }
                for (int i = 0; i < comp.length; ++i) {
                    if (comp[i] < 0) {
                        System.out.println(Integer.toString(graph.originalIdOf(i)) + "\t" + Integer.toString(-comp[i]));
                    }
                }
                break;
            case TRIANGLE:
                System.out.println("TriangleCount: " + DirectedTriangleCounter.count(graph.getGraph()));
                break;
            case UNDIRECTED_EDGE_BETWEENNESS:
                printUndirectedEdgeValues(graph, EdgeBetweennessCentrality.ofUndirected(graphArray));
                break;
            case UNDIRECTED_EDGE_LINERANK:
                printUndirectedEdgeValues(graph, UndirectedEdgeLinerank.of(graphArray));
                break;
            case UNDIRECTED_LINERANK:
                printNodeValues(graph, Linerank.ofUndirected(graphArray));
                break;
            default:
                throw new NotImplementedException("Algorithm not implemented yet");
        }
    }

}
