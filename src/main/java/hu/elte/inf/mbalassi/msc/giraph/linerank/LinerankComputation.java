package hu.elte.inf.mbalassi.msc.giraph.linerank;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class implements LineRank on Giraph. The input file should contain an adjacency list, one 
 * line for each node with the integer ID of the node, followed by the IDs of the endpoints of its
 * outgoing edges.
 * 
 */
public class LinerankComputation extends BasicComputation<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

    public static final double RESTART_PROB = 0.15;
    public static final double EPS = 0.000000001;
    
    @Override
    public void compute(Vertex<IntWritable, DoubleWritable, NullWritable> vertex, Iterable<DoubleWritable> messages)
            throws IOException {
        
        double startAtNodeProb = 1.0 / (double)this.getTotalNumEdges();
        
        // Determine new value
        if (vertex.getValue().get() < 0.0) {
            vertex.setValue(new DoubleWritable(startAtNodeProb));
            aggregate(LinerankMasterCompute.MORE_ITERATIONS_AGG, new BooleanWritable(true));
        } else {
            
            double newScore = 0.0;
            for (DoubleWritable msg : messages) {
                newScore += msg.get();
            }
            int numEdges = vertex.getNumEdges();

            if (!((BooleanWritable)getAggregatedValue(LinerankMasterCompute.MORE_ITERATIONS_AGG)).get()) {
                double finalScore = vertex.getValue().get() * numEdges + newScore;
                vertex.setValue(new DoubleWritable(finalScore));
                vertex.voteToHalt();
                return;
            }
            
            if (numEdges == 0) {
                newScore = 0.0;
            } else {
                newScore /= vertex.getNumEdges();
                newScore = startAtNodeProb * RESTART_PROB + newScore * (1.0 - RESTART_PROB);
            }
            if (Math.abs(newScore - vertex.getValue().get()) > EPS) {
                aggregate(LinerankMasterCompute.MORE_ITERATIONS_AGG, new BooleanWritable(true));
            }
            vertex.setValue(new DoubleWritable(newScore));
        }
        
        // Send value distributed among edges
        sendMessageToAllEdges(vertex, vertex.getValue());
    }
    
    private void runMain(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: <program> <input file> <output directory> <workers>");
        }
        
        GiraphConfiguration conf = new GiraphConfiguration();
        FileSystem fs = FileSystem.get(conf);
        conf.setComputationClass(getClass());
        conf.setMasterComputeClass(LinerankMasterCompute.class);
        conf.setVertexInputFormatClass(IntDoubleNullInputFormat.class);
        conf.setVertexOutputFormatClass(IntDoubleNullOutputFormat.class);
        int workers = Integer.parseInt(args[2]);
        conf.setWorkerConfiguration(workers, workers, 100.0f);
        GiraphConstants.USE_SUPERSTEP_COUNTERS.set(conf, false);
        conf.setMessageCombinerClass(IntDoubleSumCombiner.class);
        GiraphFileInputFormat.addVertexInputPath(conf, new Path(args[0]));
        
        GiraphJob giraphJob = new GiraphJob(conf, getClass().getSimpleName());
        giraphJob.getInternalJob().setJarByClass(getClass());
        Path resultPath = new Path(args[1]);
        fs.delete(resultPath, true);
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), resultPath);
        giraphJob.run(true);
    }
    
    public static void main(String[] args) throws Exception {
        new LinerankComputation().runMain(args);
    }

}
