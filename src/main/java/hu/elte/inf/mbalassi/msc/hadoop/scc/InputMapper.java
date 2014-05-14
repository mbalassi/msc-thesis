package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads the graph given as an adjacency list from file and converts it to the internal format.
 * 
 */
class InputMapper extends Mapper<LongWritable, Text, IntWritable, BytesWritable> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Read input data
        String[] idAndEdges = value.toString().split("\\s+");
        int id = Integer.parseInt(idAndEdges[0]);
        int[] edges = new int[idAndEdges.length - 1];
        for (int i = 1; i < idAndEdges.length; ++i) {
            edges[i - 1] = Integer.parseInt(idAndEdges[i]);
        }
        
        // Send node
        context.write(new IntWritable(id), MessageUtil.fromMessage(new Message(false, id, edges)));
        // Propagate labels
        for (int e : edges) {
            context.write(new IntWritable(e), MessageUtil.fromMessage(new Message(id)));
        }
    }
}
