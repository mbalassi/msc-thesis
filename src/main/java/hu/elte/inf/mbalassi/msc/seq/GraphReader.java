package hu.elte.inf.mbalassi.msc.seq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Utility class for reading graphs from file.
 * 
 */
public class GraphReader {

    /**
     * Creates a GraphReader object for the specified file. For a graph with n nodes, the input file should contain n
     * lines. Each line should contain d+1 integers separated by whitespace, where d is the outdegree of the
     * corresponding node. The first integer is the ID of the vertex, the rest are the IDs of the endpoints of the
     * outgoing edges.
     * 
     * @param inputfile the name of the input file
     */
    public GraphReader(String inputfile) throws NumberFormatException, IOException {

        // Read graph
        BufferedReader br = new BufferedReader(new FileReader(inputfile));
        String line;
        ArrayList<int[]> graph = new ArrayList<int[]>();
        int maxId = 0;
        while ((line = br.readLine()) != null) {
            String[] nums = line.split("\\s+");
            int[] edges = new int[nums.length - 1];
            for (int i = 0; i < edges.length; ++i) {
                edges[i] = Integer.parseInt(nums[i + 1]);
            }
            graph.add(edges);
            int id = Integer.parseInt(nums[0]);
            if (maxId < id) maxId = id;
        }
        br.close();

        // create sequential ids & count edges
        n = graph.size();
        m = 0;
        int[] toInternalId = new int[maxId + 1];
        fromInternalId = new int[graph.size()];
        int internalId = 0;
        br = new BufferedReader(new FileReader(inputfile));
        while ((line = br.readLine()) != null) {
            String[] nums = line.split("\\s+", 2);
            int id = Integer.parseInt(nums[0]);
            toInternalId[id] = internalId;
            fromInternalId[internalId] = id;
            internalId++;
        }
        for (int[] edges : graph) {
            m += edges.length;
            for (int i = 0; i < edges.length; ++i) {
                edges[i] = toInternalId[edges[i]];
            }
        }

        graphArray = new int[n][];
        graph.toArray(graphArray);
    }

    /**
     * Returns the parsed graph in adjacency list representation. This stored graph is returned without cloning, so it
     * should not be modified.
     * 
     * @return the parsed graph
     */
    public int[][] getGraph() {
        return graphArray;
    }

    /**
     * Get the number of nodes in the graph.
     * 
     * @return the number of nodes
     */
    public int getVertexCount() {
        return n;
    }

    /**
     * Get the number of edges in the graph.
     * 
     * @return the number of edges
     */
    public int getEdgeCount() {
        return m;
    }

    /**
     * Get the original ID of the node at the specified index in the adjacency list representation.
     * 
     * @param index the index of a node in the adjacency list
     * @return the original ID (in the parsed file) of the node
     */
    public int originalIdOf(int index) {
        return fromInternalId[index];
    }

    // graph structure
    private int[][] graphArray;
    // number of nodes
    private int n;
    // number of edges
    private int m;
    // fromInternalId[i] is the original ID of the node with internal ID i
    private int[] fromInternalId;

}
