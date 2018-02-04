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

  public class AverageCombiner
        extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
        double sum = 0;
	long count = 0;
        for (DoubleWritable val: values) {
                sum += val.get();
		count += 1;
        }
        context.write(key, new DoubleWritable(sum/count));
    }
  }

