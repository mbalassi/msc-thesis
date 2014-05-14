package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import hu.elte.inf.mbalassi.msc.hadoop.betweenness.Message.Ack;
import hu.elte.inf.mbalassi.msc.hadoop.betweenness.Message.Distance;
import hu.elte.inf.mbalassi.msc.hadoop.betweenness.Message.Score;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function for one iteration of the computation.
 * 
 */
class IteratedMapper extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

    public void map(IntWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Node node = Message.toNode(value.getBytes());
        node.id = key.get();
        for (Map.Entry<IntWritable,Node.BfsState> entry : node.bfs.entrySet()) {
            int bfsId = entry.getKey().get();
            Node.BfsState bfs = entry.getValue();
            switch (bfs.state) {
                case SENDING_DIST_MESSAGES:
                    // send acknowledgments to parents
                    for (int p : bfs.parents) {
                        Ack ack = new Ack();
                        ack.bfsId = bfsId;
                        context.write(new IntWritable(p), new BytesWritable(Message.fromAck(ack)));
                    }
                    // send dist messages on outgoing edges
                    for (int e : node.edges) {
                        Distance dist = new Distance();
                        dist.bfsId = bfsId;
                        dist.parId = node.id;
                        dist.paths = bfs.pathsTo;
                        context.write(new IntWritable(e), new BytesWritable(Message.fromDist(dist)));
                    }
                    if (node.edges.length != 0) break;
                    bfs.state = NodeState.SENDING_SCORE_MESSAGES;
                case SENDING_SCORE_MESSAGES:
                    // send score messages
                    for (int p : bfs.parents) {
                        Score sc = new Score();
                        sc.bfsId = bfsId;
                        sc.pathsTo = bfs.pathsTo;
                        sc.score = bfs.score;
                        context.write(new IntWritable(p), new BytesWritable(Message.fromScore(sc)));
                    }
                    break;
                case INACTIVE:
                case WAIT_FOR_CHILDREN_ACK:
                case WAIT_FOR_CHILDREN_SCORE:
                case FINISHED:
                    break;
            }
        }
        
        // Send node data
        context.write(key, new BytesWritable(Message.fromNode(node)));
    }

}
