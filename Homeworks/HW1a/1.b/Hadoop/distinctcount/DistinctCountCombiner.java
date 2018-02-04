import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class DistinctCountCombiner
        extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
	int count = 0;
        for (IntWritable val: values) {
		count += 1;
        }
        context.write(new IntWritable(1), new IntWritable(1));
    }
  }

