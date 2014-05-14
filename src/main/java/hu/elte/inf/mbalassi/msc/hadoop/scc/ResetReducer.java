package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for reset after an iteration of the algorithm.
 * 
 */
class ResetReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        int myLabel = -1;
        List<Integer> edgeCandidates = new ArrayList<Integer>();
        List<Integer> labels = new ArrayList<Integer>();
        // Process messages
        for (BytesWritable value : values) {
            Message msg = MessageUtil.toMessage(value);
            if (msg.type == MessageType.LABEL) {
                myLabel = msg.label;
            } else {
                labels.add(msg.label);
                edgeCandidates.add(msg.edges[0]);
            }
        }
        // Remove edges leading to other components
        int count = 0;
        for (int i : labels) {
            if (i >= 0) ++count;
        }
        int[] edges = new int[count];
        for (int i = 0; i < edgeCandidates.size(); ++i) {
            if (labels.get(i).intValue() >= 0) {
                edges[--count] = edgeCandidates.get(i);
            }
        }
        // Send node
        if (myLabel < 0) {
            context.write(key, MessageUtil.fromMessage(new Message(false, myLabel, new int[0])));
        } else {
            context.getCounter(StronglyConnectedComponents.Counters.ACTIVE_NODES).increment(1);
            context.write(key, MessageUtil.fromMessage(new Message(true, key.get(), edges)));
        }
    }
}
