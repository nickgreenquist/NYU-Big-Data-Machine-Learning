import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class LargeIntReducer
       extends Reducer<IntWritable,LongWritable,IntWritable,LongWritable> {

    public void reduce(IntWritable key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long largest = Long.MIN_VALUE;
      for (LongWritable val : values) {
        if(val.get() > largest) {
	  largest = val.get();
	}
      }
      context.write(key, new LongWritable(largest));
    }
  }
