package hu.elte.inf.mbalassi.msc.hadoop.scc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class implements calculating strongly connected components of a graph on Hadoop. The input
 * file should contain an adjacency list, one line for each node with the integer ID of the node,
 * followed by the IDs of the endpoints of its outgoing edges.
 * 
 */
public class StronglyConnectedComponents {

    static enum Counters {
        ACTIVE_NODES
    }

    /** Input file of the mapreduce job */
    String inputFile;
    /** Output file of the mapreduce job */
    String outputFile;
    /** Number of reducers */
    int numReducers;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Arguments: <input file> <output dir> <temp dir> [<reducer count>]");
            return;
        }

        String input = args[0];
        String output = args[1].endsWith("/") ? args[1] : args[1] + "/";
        String tempDir = args[2].endsWith("/") ? args[2] : args[2] + "/";
        
        int numReducers = args.length >= 3 ? Integer.parseInt(args[3]) : 0;
        Configuration conf = new Configuration();
        new StronglyConnectedComponents().run(input, output, tempDir, conf, numReducers);
    }

    private void run(String input, String output, String tempDir, Configuration conf, int numRed) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        numReducers = numRed;
        fs.delete(new Path(output), true);
        fs.delete(new Path(tempDir), true);

        int outerIter = 0;
        while (true) {
            ++outerIter;
            // Label propagation
            int innerIter = 0;
            while (true) {
                ++innerIter;
                Job job = new Job(conf, "SCC label propagation, iteration: " + outerIter + "/" + innerIter);
                boolean first = outerIter == 1 && innerIter == 1;
                inputFile = first ? input : outputFile;
                outputFile = tempDir + outerIter + "_pr_" + innerIter;
                createAndRunPropagateJob(job, first);
                if (!first) fs.delete(new Path(inputFile), true);
                if (job.getCounters().findCounter(Counters.ACTIVE_NODES).getValue() == 0) break;
            }

            // Transpose graph, remove some edges and activate nodes which kept their label
            {
                Job job = new Job(conf, "SCC transpose, iteration: " + outerIter);
                inputFile = outputFile;
                outputFile = tempDir + outerIter + "_tr";
                createAndRunTransposeJob(job);
                fs.delete(new Path(inputFile), true);
            }
            
            // Reverse label propagation
            innerIter = 0;
            while (true) {
                ++innerIter;
                Job job = new Job(conf, "SCC reverse propagation, iteration: " + outerIter + "/" + innerIter);
                inputFile = outputFile;
                outputFile = tempDir + outerIter + "_rpr_" + innerIter;
                createAndRunPropagateJob(job, false);
                fs.delete(new Path(inputFile), true);
                if (job.getCounters().findCounter(Counters.ACTIVE_NODES).getValue() == 0) break;
            }
            
            // Transpose graph, remove some edges and check if the computation finished
            Job job = new Job(conf, "SCC reset, iteration: " + outerIter);
            inputFile = outputFile;
            outputFile = tempDir + outerIter + "_tr";
            createAndRunResetJob(job);
            fs.delete(new Path(inputFile), true);
            
            // If finished, output result
            if (job.getCounters().findCounter(Counters.ACTIVE_NODES).getValue() == 0) {
                job = new Job(conf, "SCC output");
                inputFile = outputFile;
                outputFile = output;
                createAndRunOutputJob(job);
                fs.delete(new Path(inputFile), true);
                break;
            }
        }

        fs.delete(new Path(tempDir), true);
    }

    private void createAndRunPropagateJob(Job job, boolean first) throws Exception {
        // Configure mapper
        if (first) {
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InputMapper.class);
        } else {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(PropagateMapper.class);
        }
        FileInputFormat.addInputPath(job, new Path(inputFile));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // Configure combiner
        job.setCombinerClass(PropagateReducer.class);
        // Configure reducer
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setReducerClass(PropagateReducer.class);
        if (numReducers > 0) job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        // Run job
        job.setJarByClass(StronglyConnectedComponents.class);
        job.waitForCompletion(true);
    }

    private void createAndRunTransposeJob(Job job) throws Exception {
        // Configure mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(TransposeMapper.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // Configure reducer
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setReducerClass(TransposeReducer.class);
        if (numReducers > 0) job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        // Run job
        job.setJarByClass(StronglyConnectedComponents.class);
        job.waitForCompletion(true);
    }
    
    private void createAndRunResetJob(Job job) throws Exception {
        // Configure mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ResetMapper.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // Configure reducer
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setReducerClass(ResetReducer.class);
        if (numReducers > 0) job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        // Run job
        job.setJarByClass(StronglyConnectedComponents.class);
        job.waitForCompletion(true);
    }
    
    private void createAndRunOutputJob(Job job) throws Exception {
        // Configure mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(OutputMapper.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        // Configure reducer
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(OutputReducer.class);
        if (numReducers > 0) job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        // Run job
        job.setJarByClass(StronglyConnectedComponents.class);
        job.waitForCompletion(true);
    }

}
