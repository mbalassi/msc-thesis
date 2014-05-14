package hu.elte.inf.mbalassi.msc.giraph.tricnt;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleCounterComputation extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages)
            throws IOException {

        long superstep = this.getSuperstep();
        int vertexId = vertex.getId().get();

        switch ((int)superstep){
            case 0:
                for (Edge<IntWritable, NullWritable>  edge : vertex.getEdges() ){
                    IntWritable targetVertexId = edge.getTargetVertexId();
                    if (targetVertexId.get() > vertexId)
                        this.sendMessage(targetVertexId, vertex.getId());
                }
                break;
            case 1:
                //TODO consider sorted container
                for (IntWritable msg : messages){
                    for (Edge<IntWritable, NullWritable>  edge : vertex.getEdges() ){
                        IntWritable targetVertexId = edge.getTargetVertexId();
                        if (targetVertexId.get() > msg.get())
                            this.sendMessage(targetVertexId, msg);
                    }
                }
                break;
            case 2:
                long triCnt = 0;
                for (IntWritable msg : messages){
                    for (Edge<IntWritable, NullWritable>  edge : vertex.getEdges() ){
                        IntWritable targetVertexId = edge.getTargetVertexId();
                        if (targetVertexId.get() == msg.get())
                            ++triCnt;
                    }
                }
                this.aggregate(TriangleCounterMasterCompute.TRI_CNT_AGG, new LongWritable(triCnt));
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
        conf.setMasterComputeClass(TriangleCounterMasterCompute.class);
        conf.setVertexInputFormatClass(IntIntNullTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        int workers = Integer.parseInt(args[2]);
        conf.setWorkerConfiguration(workers, workers, 100.0f);
        GiraphConstants.USE_SUPERSTEP_COUNTERS.set(conf, false);
        GiraphFileInputFormat.addVertexInputPath(conf, new Path(args[0]));

        GiraphJob giraphJob = new GiraphJob(conf, getClass().getSimpleName());
        giraphJob.getInternalJob().setJarByClass(getClass());
        Path resultPath = new Path(args[1]);
        fs.delete(resultPath, true);
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), resultPath);
        giraphJob.run(true);
    }

    public static void main(String[] args) throws Exception {
        new TriangleCounterComputation().runMain(args);
    }

}
