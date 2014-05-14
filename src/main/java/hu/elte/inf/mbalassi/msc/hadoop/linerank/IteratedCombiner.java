package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combine function used in the iterative LineRank calculation
 * 
 */
public class IteratedCombiner extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        Message msg = new Message();
        for (BytesWritable value : values) {
            msg.merge(new Message(value.getBytes()));
        }
        context.write(key, new BytesWritable(msg.toBytes()));
    }

}
