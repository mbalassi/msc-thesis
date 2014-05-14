package hu.elte.inf.mbalassi.msc.giraph.scc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Writable integer pair.
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
public class SourceValueWritable implements Writable {

    private int source;
    private int value;
    
    public int getSource() { return source; }
    public int getValue() { return value; }
    
    public SourceValueWritable() {}
    
    public SourceValueWritable(int source, int value) {
        this.source = source;
        this.value = value;
    }
    
    public static SourceValueWritable from(Vertex<IntWritable, IntWritable, ?> vertex) {
        return new SourceValueWritable(vertex.getId().get(), vertex.getValue().get());
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.source = in.readInt();
        this.value = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(source);
        out.writeInt(value);
    }

}
