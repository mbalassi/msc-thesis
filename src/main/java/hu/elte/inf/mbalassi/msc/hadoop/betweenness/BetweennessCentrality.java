package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class implements betweenness centrality scores of nodes of a graph on Hadoop. The input
 * file should contain an adjacency list, one line for each node with the integer ID of the node,
 * followed by the IDs of the endpoints of its outgoing edges.
 * 
 */
public class BetweennessCentrality {
    
    public static final String INPUT_RATIO = "InputRatio"; 

    static enum Counter {
        NOT_FINISHED
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("Arguments: <input file> <output dir> <temp dir> <bfs chance> <reducer count>");
            return;
        }
        
        String input = args[0];
        String output = args[1];
        String tempDir = args[2];
        double inputRatio = Double.parseDouble(args[3]);
        if (inputRatio <= 0.0) {
            throw new IllegalArgumentException("Bfs chance should be positive!");
        }
        int reducerCount = args.length < 5 ? 0 : Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        conf.set(INPUT_RATIO, Double.toString(inputRatio));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output), true);
        fs.delete(new Path(tempDir), true);

        boolean moreIterations = true;
        boolean lastIteration = false;
        int i = 0;
        while (moreIterations || lastIteration) {
            String tempFileIn = tempDir.endsWith("/") ? tempDir + i : tempDir + "/" + i;
            String tempFileOut = tempDir.endsWith("/") ? tempDir + (i+1) : tempDir + "/" + (i+1);

            Job job = new Job(conf, "betweenness centrality calculation, iteration: " + (i + 1));
            
            if (i == 0) {
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(InputMapper.class);
                FileInputFormat.addInputPath(job, new Path(input));
            } else {
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setMapperClass(IteratedMapper.class);
                FileInputFormat.addInputPath(job, new Path(tempFileIn));
            }

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BytesWritable.class);

            if (!lastIteration) {
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(BytesWritable.class);
                job.setReducerClass(IteratedReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(tempFileOut));
            } else {
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setReducerClass(OutputReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(output));
            }
            
            if (reducerCount > 0) {
                job.setNumReduceTasks(reducerCount);
            }
            job.setJarByClass(BetweennessCentrality.class);
            job.waitForCompletion(true);
            
            if (lastIteration) {
                lastIteration = false;
            } else {
                moreIterations = i == 0 ? true :
                    job.getCounters().findCounter(Counter.NOT_FINISHED).getValue() > 0;
                lastIteration = !moreIterations;
            }
            
            if (i > 0) {
                fs.delete(new Path(tempFileIn), true);
            }
            ++i;
        }
        fs.delete(new Path(tempDir), true);
    }

}
