package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for creating the results after the calculation is done.
 * 
 */
public class OutputReducer extends Reducer<IntWritable, BytesWritable, IntWritable, DoubleWritable> {
    
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        double score = 0.0;
        for (BytesWritable value : values) {
            score += new Message(value.getBytes()).score;
        }
        context.write(key, new DoubleWritable(score));
    }

}
