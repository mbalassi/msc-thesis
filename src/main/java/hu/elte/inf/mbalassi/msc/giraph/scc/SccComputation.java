package hu.elte.inf.mbalassi.msc.giraph.scc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class implements strongly connected component calculation on Giraph. The input file should
 * contain an adjacency list, one line for each node with the integer ID of the node, followed by
 * the IDs of the endpoints of its outgoing edges.
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
public class SccComputation extends BasicComputation<IntWritable, IntWritable, NullWritable, SourceValueWritable> {
    
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<SourceValueWritable> messages)
            throws IOException {
        
        // First superstep
        if (getSuperstep() == 0) {
            // Start propagation
            SourceValueWritable message = SourceValueWritable.from(vertex);
            sendMessageToAllEdges(vertex, message);
            return;
        }
        
        ComputationPhase phase = ComputationPhase.fromInt(((IntWritable)getAggregatedValue(SccMasterCompute.PHASE_AGG)).get());
        switch (phase) {
            case LABEL_PROPAGATION:
            case REVERSE_LABEL_PROPAGATION:
                // Process messages
                int minReceived = Integer.MAX_VALUE;
                for (SourceValueWritable msg : messages) {
                    int cur = msg.getValue();
                    if (cur < minReceived) {
                        minReceived = cur;
                    }
                }
                // If label changed, propagate
                if (minReceived < vertex.getValue().get()) {
                    aggregate(SccMasterCompute.MORE_ITERATIONS_AGG, new BooleanWritable(true));
                    vertex.setValue(new IntWritable(minReceived));
                    sendMessageToAllEdges(vertex, SourceValueWritable.from(vertex));
                }
                break;
            case TRANSPOSE_GRAPH:
            case RESET_GRAPH:
                // Send soon-to-be edge end points w/ node value
                if (0 <= vertex.getValue().get()) {
                    SourceValueWritable message = SourceValueWritable.from(vertex);
                    sendMessageToAllEdges(vertex, message);
                }
                // Delete edges
                vertex.setEdges(new ArrayList<Edge<IntWritable, NullWritable>>());
                break;
            case INIT_REVERSE_LABEL_PROPAGATION:
                // Receive edges
                for (SourceValueWritable msg : messages) {
                    // Edges between nodes in different components can be removed
                    if (msg.getValue() == vertex.getValue().get()) {
                        vertex.addEdge(EdgeFactory.create(new IntWritable(msg.getSource())));
                    }
                }
                // Initiate reverse propagation
                if (vertex.getValue().get() == vertex.getId().get()) {
                    vertex.setValue(new IntWritable(-vertex.getValue().get()-1));
                    sendMessageToAllEdges(vertex, SourceValueWritable.from(vertex));
                }
                break;
            case INIT_LABEL_PROPAGATION:
                // Nodes with determined SCC can become inactive
                if (0 <= vertex.getValue().get()) {
                    // Receive edges
                    for (SourceValueWritable msg : messages) {
                        vertex.addEdge(EdgeFactory.create(new IntWritable(msg.getSource())));
                    }
                    // Send label
                    vertex.setValue(vertex.getId());
                    sendMessageToAllEdges(vertex, SourceValueWritable.from(vertex));
                    aggregate(SccMasterCompute.MORE_ITERATIONS_AGG, new BooleanWritable(true));
                }
                break;
            case INIT_COUNT:
                // Send count to representative node
                int realValue = -(vertex.getValue().get()+1);
                if (realValue == vertex.getId().get()) {
                    vertex.setValue(new IntWritable(1));
                } else {
                    sendMessage(new IntWritable(realValue), SourceValueWritable.from(vertex));
                    vertex.setValue(new IntWritable(0));
                }
                break;
            case COUNT:
                int val = vertex.getValue().get();
                for (@SuppressWarnings("unused") SourceValueWritable msg : messages) {
                    ++val;
                }
                vertex.setValue(new IntWritable(val));
                vertex.voteToHalt();
                break;
        }
    }
    
    private void runMain(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: <program> <input file> <output directory> <workers>");
        }
        
        GiraphConfiguration conf = new GiraphConfiguration();
        FileSystem fs = FileSystem.get(conf);
        conf.setComputationClass(getClass());
        conf.setMasterComputeClass(SccMasterCompute.class);
        conf.setVertexInputFormatClass(IntIntNullTextInputFormat.class);
        conf.setVertexOutputFormatClass(IntNoZeroTextOutputFormat.class);
        int workers = Integer.parseInt(args[2]);
        conf.setWorkerConfiguration(workers, workers, 100.0f);
        GiraphConstants.USE_SUPERSTEP_COUNTERS.set(conf, false);
        GiraphFileInputFormat.addVertexInputPath(conf, new Path(args[0]));
        
        GiraphJob giraphJob = new GiraphJob(conf, getClass().getCanonicalName());
        giraphJob.getInternalJob().setJarByClass(getClass());
        Path resultPath = new Path(args[1]);
        fs.delete(resultPath, true);
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), resultPath);
        giraphJob.run(true);
    }
    
    public static void main(String[] args) throws Exception {      
        new SccComputation().runMain(args);
    }

}
