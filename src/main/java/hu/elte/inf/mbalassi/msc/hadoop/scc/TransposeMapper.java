package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for transposing all and removing some edges between the forward and backward label propagations.
 * 
 */
class TransposeMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message node = MessageUtil.toMessage(value);
        // Send node label
        context.write(key, MessageUtil.fromMessage(new Message(node.label)));
        // Send reversed edges
        for (int e : node.edges) {
            context.write(new IntWritable(e),
                    MessageUtil.fromMessage(new Message(false, node.label, new int[] {key.get()})));
        }
    }

}
