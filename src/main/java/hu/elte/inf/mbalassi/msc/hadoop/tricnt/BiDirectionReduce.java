package hu.elte.inf.mbalassi.msc.hadoop.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BiDirectionReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

  public void reduce(LongWritable id, Iterable<Text> destsOrSrcs, Context context)
    throws IOException, InterruptedException {

  String destStr = "";
  String srcStr = "";
  
  for (Text dos: destsOrSrcs)
  {
     String[] pair = dos.toString().split(" ");
     if (pair[0].equals("d")) 
       destStr += pair[1] + " ";
     if (pair[0].equals("s")) 
       srcStr += pair[1] + " ";
  }
  //id with its out- and inedges
  context.write(id, new Text(destStr + "|" + srcStr));
  }
}
