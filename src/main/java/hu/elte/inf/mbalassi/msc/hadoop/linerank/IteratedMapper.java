package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for one iteration of the LineRank calculation
 * 
 */
public class IteratedMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    
    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message node = new Message(value.getBytes());
        if (node.data.startAtNodeProb == 0.0) { // Initialize node
            node.data.lastscore = node.data.startAtNodeProb =
                    1.0 / (double)context.getConfiguration().getLong(Linerank.EDGE_COUNT, 0);
        }
        context.write(key, new BytesWritable(node.toBytes()));
        Message score = new Message();
        score.score = node.data.lastscore;
        BytesWritable scoreMsg = new BytesWritable(score.toBytes());
        for (int target : node.data.edges) {
            context.write(new IntWritable(target), scoreMsg);
        }
    }
    
}
