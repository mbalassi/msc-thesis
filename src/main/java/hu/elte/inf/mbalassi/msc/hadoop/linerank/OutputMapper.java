package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for creating the results after the calculation is done.
 * 
 */
public class OutputMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message msg = new Message(value.getBytes());
        msg.score = msg.data.lastscore;
        int[] edges = msg.data.edges;
        msg.data = null;
        BytesWritable scoreMsg = new BytesWritable(msg.toBytes());
        // send the contribution of each edge to each source
        for (int i : edges) {
            context.write(new IntWritable(i), scoreMsg);
        }
        // each contributes to their target (this node)
        msg.score *= edges.length;
        context.write(key, new BytesWritable(msg.toBytes()));
    }

}
