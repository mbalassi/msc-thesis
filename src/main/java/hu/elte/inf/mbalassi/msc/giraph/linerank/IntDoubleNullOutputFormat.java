package hu.elte.inf.mbalassi.msc.giraph.linerank;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Integer ID and double value output format.
 * 
 */
public class IntDoubleNullOutputFormat extends TextVertexOutputFormat<IntWritable, DoubleWritable, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        return new VertexDataWriter();
    }
    
    public class VertexDataWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<IntWritable, DoubleWritable, NullWritable> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(
                    new Text(Integer.toString(vertex.getId().get())),
                    new Text(Double.toString(vertex.getValue().get())));
        }
        
    }
    
}
