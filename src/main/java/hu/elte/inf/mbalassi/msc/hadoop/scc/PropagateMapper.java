package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for an iteration of label propagation.
 * 
 */
class PropagateMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message node = MessageUtil.toMessage(value);
        // Send node
        context.write(key, MessageUtil.fromMessage(new Message(false, node.label, node.edges)));
        // Propagate
        if (node.type == MessageType.ACTIVE_NODE) {
            for (int e : node.edges) {
                context.write(new IntWritable(e), MessageUtil.fromMessage(new Message(node.label)));
            }
        }
    }

}
