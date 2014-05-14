package hu.elte.inf.mbalassi.msc.hadoop.scc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for creating the output after the computation is complete
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
class OutputReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        int size = 0;
        for (@SuppressWarnings("unused") NullWritable value : values) {
            ++size;
        }
        context.write(key, new IntWritable(size));
    }
}
