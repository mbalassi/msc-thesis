package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import hu.elte.inf.mbalassi.msc.hadoop.betweenness.BetweennessCentrality.Counter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function for one iteration of the computation.
 * 
 */
class IteratedReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException,
            InterruptedException {
        // unpack node data
        List<byte[]> cache = new ArrayList<byte[]>();
        Node node = null;
        for (BytesWritable value : values) {
            byte[] message = Arrays.copyOf(value.getBytes(), value.getLength());
            if (Message.typeOf(message) == MessageType.NODE) {
                node = Message.toNode(message);
            } else {
                cache.add(message);
            }
        }
        node.id = key.get();

        // process messages
        List<byte[]> cachedDistMsgs = new ArrayList<byte[]>();
        for (byte[] message : cache) {
            MessageType msgType = Message.typeOf(message);
            Node.BfsState bfs;
            switch (msgType) {
                case DISTANCE:
                    Message.Distance dist = Message.toDist(message);
                    IntWritable bfsId = new IntWritable(dist.bfsId);
                    bfs = node.bfs.get(bfsId);
                    if (bfs == null) {
                        bfs = new Node.BfsState();
                        node.bfs.put(bfsId, bfs);
                    }
                    if (bfs.state == NodeState.INACTIVE) {
                        bfs.pathsTo += dist.paths;
                        bfs.children += 1; // store number of parents temporarily
                        cachedDistMsgs.add(message);
                    }
                    break;
                case ACKNOWLEDGE_ROUTE:
                    Message.Ack ack = Message.toAck(message);
                    bfs = node.bfs.get(new IntWritable(ack.bfsId));
                    if (bfs.state == NodeState.WAIT_FOR_CHILDREN_ACK) {
                        bfs.children++;
                    } else {
                        throw new IllegalStateException("Reducer received unexpected ack");
                    }
                    break;
                case SCORE:
                    Message.Score sc = Message.toScore(message);
                    bfs = node.bfs.get(new IntWritable(sc.bfsId));
                    if (bfs.state == NodeState.WAIT_FOR_CHILDREN_ACK
                            || bfs.state == NodeState.WAIT_FOR_CHILDREN_SCORE) {
                        bfs.score += (sc.score + 1.0) * (bfs.pathsTo / sc.pathsTo);
                        bfs.children--;
                    } else {
                        throw new IllegalStateException("Reducer received unexpected score");
                    }
                    break;
                case NODE:
                    throw new AssertionError();
            }
        }
        
        // Advance state where necessary
        for (Node.BfsState bfs : node.bfs.values()) {
            switch (bfs.state) {
                case SENDING_DIST_MESSAGES:
                    bfs.state = NodeState.WAIT_FOR_CHILDREN_ACK;
                    break;
                case WAIT_FOR_CHILDREN_ACK:
                    bfs.state = NodeState.WAIT_FOR_CHILDREN_SCORE;
                    break;
                case SENDING_SCORE_MESSAGES:
                    bfs.state = NodeState.FINISHED;
                    break;
                default:
                    break;
            }
        }

        // Finish storing parent nodes
        if (cachedDistMsgs.size() > 0) {
            for (Node.BfsState bfs : node.bfs.values()) {
                if (bfs.state == NodeState.INACTIVE && bfs.children != 0) {
                    bfs.parents = new int[bfs.children];
                    bfs.state = NodeState.SENDING_DIST_MESSAGES;
                }
            }
            // Go through messages again to fill parent list
            for (byte[] message : cachedDistMsgs) {
                Message.Distance dist = Message.toDist(message);
                Node.BfsState bfs = node.bfs.get(new IntWritable(dist.bfsId));
                bfs.parents[--bfs.children] = dist.parId;
            }
        }

        // Check if any score calculation is done
        for (Map.Entry<IntWritable,Node.BfsState> entry : node.bfs.entrySet()) {
            Node.BfsState bfs = entry.getValue();
            if (bfs.state == NodeState.WAIT_FOR_CHILDREN_SCORE && bfs.children == 0) {
                if (entry.getKey().get() == node.id) {
                    bfs.state = NodeState.FINISHED;
                } else {
                    bfs.state = NodeState.SENDING_SCORE_MESSAGES;
                    node.finalScore += bfs.score;
                }
            }
        }

        // Output node
        context.write(key, new BytesWritable(Message.fromNode(node)));
        
        // Check if more iterations are needed
        Node.BfsState bfs = node.bfs.get(new IntWritable(node.id));
        if (bfs != null && bfs.state != NodeState.FINISHED) {
            context.getCounter(Counter.NOT_FINISHED).increment(1);
        }
    }
}
