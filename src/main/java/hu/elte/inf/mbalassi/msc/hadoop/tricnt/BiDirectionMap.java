package hu.elte.inf.mbalassi.msc.hadoop.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//forwards and all outedges and inedges from higher smaller ids
public class BiDirectionMap extends Mapper<LongWritable, Text, LongWritable, Text>
{
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException
      {
        String src;

        String line = value.toString();
        String num = "";
        int idx = 0;
        while (idx < line.length() && line.charAt(idx) != ' ') {
          num += line.charAt(idx++);
        }
        src = num;
        num = "";
        ++idx;
        while (idx < line.length()) {
          num = "";
          while (idx < line.length() && line.charAt(idx) != ' ') {
            num += line.charAt(idx++);
          }
          context.write(new LongWritable(Long.parseLong(src)), new Text("d " + num));
          if (num.compareTo(src) > 0){
        	  context.write(new LongWritable(Long.parseLong(num)), new Text("s " + src));
          }
          num = "";
          ++idx;
        }
      }
}
