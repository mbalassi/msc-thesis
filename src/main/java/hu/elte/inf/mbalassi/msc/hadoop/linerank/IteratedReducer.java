package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import hu.elte.inf.mbalassi.msc.hadoop.linerank.Linerank.Counter;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for one iteration of the LineRank calculation
 * 
 */
public class IteratedReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        Message msg = new Message();
        for (BytesWritable value : values) {
            msg.merge(new Message(value.getBytes()));
        }
        
        if (msg.data.edges.length != 0) {
            msg.score /= (double)msg.data.edges.length;
        } else {
            msg.score = 0.0;
        }
        msg.score = msg.data.startAtNodeProb * Linerank.RESTART_PROB
                + msg.score * (1.0 - Linerank.RESTART_PROB);
        if (Math.abs(msg.score - msg.data.lastscore) > Linerank.EPS) {
            context.getCounter(Counter.MORE_ITERATIONS).increment(1);
        }
        msg.data.lastscore = msg.score;
        msg.score = 0.0;
        
        context.write(key, new BytesWritable(msg.toBytes()));
    }

}
