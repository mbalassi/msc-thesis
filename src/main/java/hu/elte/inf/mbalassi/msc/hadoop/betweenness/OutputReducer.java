package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Creates the result after the computation is done.
 *
 */
class OutputReducer extends Reducer<IntWritable, BytesWritable, IntWritable, DoubleWritable> {

    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        Node node = Message.toNode(values.iterator().next().getBytes());
        double inputRatio = Double.parseDouble(context.getConfiguration().get(BetweennessCentrality.INPUT_RATIO));
        context.write(key, new DoubleWritable(node.finalScore / inputRatio));
    }

}