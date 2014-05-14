package hu.elte.inf.mbalassi.msc.giraph.betweenness;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class to write the result of a betweenness centrality computation to file.
 * 
 */
public class VertexDataOutputFormat extends TextVertexOutputFormat<IntWritable, VertexData, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        return new VertexDataWriter();
    }

    public class VertexDataWriter extends TextVertexWriterToEachLine {
        @Override
        protected Text convertVertexToLine(Vertex<IntWritable, VertexData, NullWritable> vertex) throws IOException {
            return new Text(vertex.getId().toString() + "\t" + vertex.getValue().finalScore);
        }
    }
    
}
