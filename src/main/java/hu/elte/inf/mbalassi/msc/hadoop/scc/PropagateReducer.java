package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for an iteration of label propagation.
 * 
 */
public class PropagateReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        // Initialize
        boolean isActive = false;
        int myLabel = -1;
        int bestLabel = Integer.MAX_VALUE;
        int[] edges = null;
        // Process messages
        for (BytesWritable value : values) {
            Message msg = MessageUtil.toMessage(value);
            switch (msg.type) {
                case LABEL:
                    if (msg.label < bestLabel) bestLabel = msg.label;
                    break;
                case ACTIVE_NODE:
                    isActive = true;
                case INACTIVE_NODE:
                    myLabel = msg.label;
                    edges = msg.edges;
                    break;
            }
        }
        if (edges == null) {
            // Happens only in combiner
            context.write(key, MessageUtil.fromMessage(new Message(bestLabel)));
        } else {
            if (myLabel > bestLabel) {
                isActive = true;
            } else {
                bestLabel = myLabel;
            }
            if (isActive) {
                context.getCounter(StronglyConnectedComponents.Counters.ACTIVE_NODES).increment(1);
            }
            context.write(key, MessageUtil.fromMessage(new Message(isActive, bestLabel, edges)));
        }
    }

}
