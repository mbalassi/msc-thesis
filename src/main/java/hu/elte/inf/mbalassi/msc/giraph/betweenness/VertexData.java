package hu.elte.inf.mbalassi.msc.giraph.betweenness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Internal value class to store the state of a vertex.
 * 
 */
public class VertexData implements Writable {
    
    static class BfsState {
        int children;
        VertexState state = VertexState.INACTIVE;
        double pathsTo;
        double score;
        int[] parents = new int[0];
        
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(children);
            out.write(state.toByte());
            out.writeDouble(pathsTo);
            out.writeDouble(score);
            out.writeInt(parents.length);
            for (int i = 0; i < parents.length; ++i) {
                out.writeInt(parents[i]);
            }
        }
        
        public static BfsState readFrom(DataInput buf) throws IOException {
            BfsState bfs = new BfsState();
            bfs.children = buf.readInt();
            bfs.state = VertexState.fromByte(buf.readByte());
            bfs.pathsTo = buf.readDouble();
            bfs.score = buf.readDouble();
            bfs.parents = new int[buf.readInt()];
            for (int i = 0; i < bfs.parents.length; ++i) {
                bfs.parents[i] = buf.readInt();
            }
            return bfs;
        }
    }
    
    double finalScore;
    Map<IntWritable, BfsState> bfs = new HashMap<IntWritable, BfsState>();
    
    @Override
    public void readFields(DataInput in) throws IOException {
        finalScore = in.readDouble();
        int n = in.readInt();
        for (int i = 0; i < n; ++i) {
            int id = in.readInt();
            bfs.put(new IntWritable(id), BfsState.readFrom(in));
        }
        
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(finalScore);
        out.writeInt(bfs.size());
        for (Map.Entry<IntWritable,BfsState> entry : bfs.entrySet()) {
            out.writeInt(entry.getKey().get());
            entry.getValue().writeTo(out);
        }
    }
}
