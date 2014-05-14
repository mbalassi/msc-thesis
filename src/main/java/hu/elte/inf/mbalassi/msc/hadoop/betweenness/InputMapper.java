package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads the graph given as an adjacency list from file and converts it to the internal format.
 * 
 */
class InputMapper extends Mapper<LongWritable, Text, IntWritable, BytesWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Initialize node
        Node node = new Node();
        String[] idAndEdges = value.toString().split("\\s+");
        node.id = Integer.parseInt(idAndEdges[0]);
        node.edges = new int[idAndEdges.length - 1];
        for (int i = 1; i < idAndEdges.length; ++i) {
            node.edges[i - 1] = Integer.parseInt(idAndEdges[i]);
        }
        node.finalScore = 0.0;
        node.bfs = new HashMap<IntWritable, Node.BfsState>();
        // Send first messages in its own BFS tree
        if (new Random().nextDouble() < Double.parseDouble(context.getConfiguration().get(BetweennessCentrality.INPUT_RATIO))) {
            for (int e : node.edges) {
                Message.Distance dist = new Message.Distance();
                dist.bfsId = node.id;
                dist.parId = node.id;
                dist.paths = 1.0;
                context.write(new IntWritable(e), new BytesWritable(Message.fromDist(dist)));
            }
            Node.BfsState bfs = new Node.BfsState();
            bfs.state = NodeState.SENDING_DIST_MESSAGES;
            node.bfs.put(new IntWritable(node.id), bfs);
        }
        // Send node data
        context.write(new IntWritable(node.id), new BytesWritable(Message.fromNode(node)));
    }
}
