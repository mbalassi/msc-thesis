package hu.elte.inf.mbalassi.msc.hadoop.betweenness;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

/**
 * Internal value class to store a node's state.
 * 
 */
class Node {
    
    static class BfsState {
        int children;
        NodeState state = NodeState.INACTIVE;
        double pathsTo;
        double score;
        int[] parents = new int[0];
        
        public int sizeInBytes() {
            return 4 // children
                    + 1 // state
                    + 8 // pathsTo
                    + 8 // score
                    + 4 // parents.length
                    + 4 * parents.length;
        }
        
        public void writeTo(ByteBuffer buf) {
            buf.putInt(children);
            buf.put(state.toByte());
            buf.putDouble(pathsTo);
            buf.putDouble(score);
            buf.putInt(parents.length);
            buf.asIntBuffer().put(parents);
            buf.position(buf.position() + 4*parents.length);
        }
        
        public static BfsState readFrom(ByteBuffer buf) {
            BfsState bfs = new BfsState();
            bfs.children = buf.getInt();
            bfs.state = NodeState.fromByte(buf.get());
            bfs.pathsTo = buf.getDouble();
            bfs.score = buf.getDouble();
            bfs.parents = new int[buf.getInt()];
            buf.asIntBuffer().get(bfs.parents);
            buf.position(buf.position() + 4*bfs.parents.length);
            return bfs;
        }
    }
    
    int id; // ID of the node
    double finalScore;
    int[] edges;
    Map<IntWritable, BfsState> bfs;
}
