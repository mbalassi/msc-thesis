package hu.elte.inf.mbalassi.msc.hadoop.linerank;

import hu.elte.inf.mbalassi.msc.hadoop.linerank.Linerank.Counter;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads the graph given as an adjacency list from file and converts it to the internal format.
 * 
 */
public class InputMapper extends Mapper<LongWritable, Text, IntWritable, BytesWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        IntWritable id = new IntWritable(Integer.parseInt(tokens[0]));
       
        // Initialize edges
        Message.NodeData node = new Message.NodeData();
        node.edges = new int[tokens.length-1];
        for (int i = 1; i < tokens.length; ++i) {
            node.edges[i-1] = Integer.parseInt(tokens[i]);
        }
        context.getCounter(Counter.EDGE_COUNT).increment(node.edges.length);

        // Send node data
        Message msg = new Message();
        msg.data = node;
        context.write(id, new BytesWritable(msg.toBytes()));
    }
}
