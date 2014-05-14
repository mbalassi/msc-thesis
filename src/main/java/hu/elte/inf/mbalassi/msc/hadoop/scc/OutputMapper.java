package hu.elte.inf.mbalassi.msc.hadoop.scc;

import hu.elte.inf.mbalassi.msc.hadoop.scc.MessageUtil.Message;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for creating the output after the computation is complete
 * 
 */
class OutputMapper extends Mapper<IntWritable, BytesWritable, IntWritable, NullWritable> {
    
    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message node = MessageUtil.toMessage(value);
        context.write(new IntWritable(-(node.label+1)), NullWritable.get());
    }

}
