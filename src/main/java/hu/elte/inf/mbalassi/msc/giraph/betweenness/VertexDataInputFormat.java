package hu.elte.inf.mbalassi.msc.giraph.betweenness;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Class for reading graph topology from a file. Each line should describe a vertex in "id
 * edge_target_id1 edge_target_id2 ..." format. The {@link VertexData} returned is uninitialized.
 * 
 */
public class VertexDataInputFormat extends TextVertexInputFormat<IntWritable, VertexData, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SimpleVertexReader();
    }
    
    public class SimpleVertexReader extends TextVertexReaderFromEachLine {

        @Override
        protected Iterable<Edge<IntWritable, NullWritable>> getEdges(Text line) throws IOException {
            String[] ints = line.toString().split("\\s+");
            List<Edge<IntWritable, NullWritable>> edges = new ArrayList<Edge<IntWritable, NullWritable>>();
            for (int i = 1; i < ints.length; ++i) {
                edges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(ints[i]))));
            }
            return edges;
        }

        @Override
        protected IntWritable getId(Text line) throws IOException {
            return new IntWritable(Integer.parseInt(line.toString().split("\\s+",2)[0]));
        }

        @Override
        protected VertexData getValue(Text line) throws IOException {
            return new VertexData();
        }
        
    }

}
