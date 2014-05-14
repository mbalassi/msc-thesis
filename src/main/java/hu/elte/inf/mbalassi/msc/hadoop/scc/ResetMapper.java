package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for reset after an iteration of the algorithm.
 * 
 */
class ResetMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message node = MessageUtil.toMessage(value);
        // Send node label
        context.write(key, MessageUtil.fromMessage(new Message(node.label)));
        // Send reversed edges
        if (node.label >= 0) { // remove edges from finished nodes
            for (int e : node.edges) {
                context.write(new IntWritable(e),
                        MessageUtil.fromMessage(new Message(false, node.label, new int[] {key.get()})));
            }
        }
    }
    
}
