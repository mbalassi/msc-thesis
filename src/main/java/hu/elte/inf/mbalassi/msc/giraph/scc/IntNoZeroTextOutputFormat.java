package hu.elte.inf.mbalassi.msc.giraph.scc;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Integer ID and integer value output format where zero values are omitted.
 * 
 * @author Péter Englert <engi.peti@gmail.com>
 */
public class IntNoZeroTextOutputFormat extends TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        return new VertexDataWriter();
    }

    public class VertexDataWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<IntWritable, IntWritable, NullWritable> vertex) throws IOException,
                InterruptedException {
            if (0 != vertex.getValue().get()) {
                getRecordWriter().write(
                        new Text(Integer.toString(vertex.getId().get())),
                        new Text(Integer.toString(vertex.getValue().get())));
            }
        }
        
    }
    
}
