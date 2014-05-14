package hu.elte.inf.mbalassi.msc.giraph.betweenness;


import hu.elte.inf.mbalassi.msc.giraph.betweenness.VertexData.BfsState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Giraph computation class for calculating the betweenness centrality of each node.
 * 
 */
public class BetweennessComputation
    extends BasicComputation<IntWritable, VertexData, NullWritable, BytesWritable> {

    @Override
    public void compute(Vertex<IntWritable, VertexData, NullWritable> vertex, Iterable<BytesWritable> messages)
            throws IOException {
        
        VertexData data = vertex.getValue();
        int id = vertex.getId().get();

        // process messages
        List<MessageUtil.Distance> cachedDistMsgs = new ArrayList<MessageUtil.Distance>();
        for (BytesWritable msg : messages) {
            byte[] message = msg.getBytes();
            BfsState bfsState;
            IntWritable bfsId;
            switch (MessageUtil.typeOf(message)) {
                case DISTANCE:
                    MessageUtil.Distance dist = MessageUtil.toDist(message);
                    bfsId = new IntWritable(dist.bfsId);
                    bfsState = data.bfs.get(bfsId);
                    if (bfsState == null) {
                        bfsState = new BfsState();
                        data.bfs.put(bfsId, bfsState);
                    }
                    if (bfsState.state == VertexState.INACTIVE) {
                        bfsState.pathsTo += dist.paths;
                        bfsState.children += 1; // store number of parents temporarily
                        cachedDistMsgs.add(dist);
                    }
                    break;
                case ACKNOWLEDGE_ROUTE:
                    MessageUtil.Ack ack = MessageUtil.toAck(message);
                    bfsState = data.bfs.get(new IntWritable(ack.bfsId));
                    if (bfsState.state == VertexState.WAIT_FOR_CHILDREN_ACK) {
                        bfsState.children++;
                    } else {
                        throw new IllegalStateException("Reducer received unexpected ack");
                    }
                    break;
                case SCORE:
                    MessageUtil.Score sc = MessageUtil.toScore(message);
                    bfsState = data.bfs.get(new IntWritable(sc.bfsId));
                    if (bfsState.state == VertexState.WAIT_FOR_CHILDREN_ACK
                            || bfsState.state == VertexState.WAIT_FOR_CHILDREN_SCORE) {
                        bfsState.score += (sc.score + 1.0) * (bfsState.pathsTo / sc.pathsTo);
                        bfsState.children--;
                    } else {
                        throw new IllegalStateException("Reducer received unexpected score");
                    }
                    break;
            }
        }
        
        // Advance state where necessary
        for (BfsState bfsState : data.bfs.values()) {
            switch (bfsState.state) {
                case SENDING_DIST_MESSAGES:
                    bfsState.state = VertexState.WAIT_FOR_CHILDREN_ACK;
                    break;
                case WAIT_FOR_CHILDREN_ACK:
                    bfsState.state = VertexState.WAIT_FOR_CHILDREN_SCORE;
                    break;
                default:
                    break;
            }
        }

        // Finish storing parent nodes
        if (this.getSuperstep() == 0) {
            if (new Random().nextFloat() < getConf().getFloat(INPUT_RATIO, 1.0f)) {
                BfsState bfsState = new BfsState();
                bfsState.pathsTo = 1.0;
                bfsState.state = VertexState.SENDING_DIST_MESSAGES;
                data.bfs.put(vertex.getId(), bfsState);
            }
        }
        if (cachedDistMsgs.size() > 0) {
            for (BfsState bfsState : data.bfs.values()) {
                if (bfsState.state == VertexState.INACTIVE && bfsState.children != 0) {
                    bfsState.parents = new int[bfsState.children];
                    bfsState.state = VertexState.SENDING_DIST_MESSAGES;
                }
            }
            // Go through messages again to fill parent list
            for (MessageUtil.Distance dist : cachedDistMsgs) {
                BfsState bfsState = data.bfs.get(new IntWritable(dist.bfsId));
                bfsState.parents[--bfsState.children] = dist.parId;
            }
        }

        // Check if any score calculation is done
        for (Map.Entry<IntWritable, BfsState> entry : data.bfs.entrySet()) {
            BfsState bfsState = entry.getValue();
            if (bfsState.state == VertexState.WAIT_FOR_CHILDREN_SCORE && bfsState.children == 0) {
                if (entry.getKey().get() == id) {
                    bfsState.state = VertexState.FINISHED;
                } else {
                    bfsState.state = VertexState.SENDING_SCORE_MESSAGES;
                    data.finalScore += bfsState.score;
                }
            }
        }
        
        // Send messages
        for (Map.Entry<IntWritable, BfsState> entry : data.bfs.entrySet()) {
            int bfsId = entry.getKey().get();
            BfsState bfsState = entry.getValue();
            switch (bfsState.state) {
                case SENDING_DIST_MESSAGES:
                    // send acknowledgments to parents
                    for (int p : bfsState.parents) {
                        MessageUtil.Ack ack = new MessageUtil.Ack();
                        ack.bfsId = bfsId;
                        sendMessage(new IntWritable(p), new BytesWritable(MessageUtil.fromAck(ack)));
                    }
                    // send dist messages on outgoing edges
                    for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
                        MessageUtil.Distance dist = new MessageUtil.Distance();
                        dist.bfsId = bfsId;
                        dist.parId = id;
                        dist.paths = bfsState.pathsTo;
                        sendMessage(e.getTargetVertexId(), new BytesWritable(MessageUtil.fromDist(dist)));
                    }
                    if (vertex.getNumEdges() != 0) break;
                    bfsState.state = VertexState.SENDING_SCORE_MESSAGES;
                case SENDING_SCORE_MESSAGES:
                    // send score messages
                    for (int p : bfsState.parents) {
                        MessageUtil.Score sc = new MessageUtil.Score();
                        sc.bfsId = bfsId;
                        sc.pathsTo = bfsState.pathsTo;
                        sc.score = bfsState.score;
                        sendMessage(new IntWritable(p), new BytesWritable(MessageUtil.fromScore(sc)));
                    }
                    bfsState.state = VertexState.FINISHED;
                    break;
                case INACTIVE:
                case WAIT_FOR_CHILDREN_ACK:
                case WAIT_FOR_CHILDREN_SCORE:
                case FINISHED:
                    break;
            }
        }
        
        // Check if more iterations are needed
        boolean halt = true;
        for (BfsState bfsState : data.bfs.values()) {
            if (bfsState.state != VertexState.FINISHED) {
                halt = false;
                break;
            }
        }
        if (halt) {
            vertex.voteToHalt();
        }
    }
    
    public static final String INPUT_RATIO = "INPUT_RATIO";
    
    private void runMain(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: <program> <input file> <output directory> <bfs chance> <workers>");
            return;
        }
        
        GiraphConfiguration conf = new GiraphConfiguration();
        FileSystem fs = FileSystem.get(conf);
        conf.setComputationClass(getClass());
        conf.setVertexInputFormatClass(VertexDataInputFormat.class);
        conf.setVertexOutputFormatClass(VertexDataOutputFormat.class);
        float inputRatio = Float.parseFloat(args[2]);
        if (inputRatio <= 0.0) {
            throw new IllegalArgumentException("Bfs chance should be positive");
        }
        conf.setFloat(INPUT_RATIO, inputRatio);
        int workers = Integer.parseInt(args[3]);
        conf.setWorkerConfiguration(workers, workers, 100.0f);
        GiraphFileInputFormat.addVertexInputPath(conf, new Path(args[0]));
        
        GiraphJob giraphJob = new GiraphJob(conf, getClass().getCanonicalName());
        giraphJob.getInternalJob().setJarByClass(getClass());
        Path resultPath = new Path(args[1]);
        fs.delete(resultPath, true);
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), resultPath);
        giraphJob.run(true);
    }
    
    public static void main(String[] args) throws Exception {      
        new BetweennessComputation().runMain(args);
    }

}
