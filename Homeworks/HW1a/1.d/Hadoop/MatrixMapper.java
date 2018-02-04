import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
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

  public class MatrixMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	int m = Integer.parseInt(conf.get("m"));
	String line = value.toString();

	String[] csv = line.split(",");
	
	Text outKey = new Text();
	Text outValue = new Text();

	if(csv[0].equals("M")) {
		outKey.set(csv[1] + "," + 0);
		outValue.set(csv[0] + "," + csv[2] + "," + csv[3]);
		context.write(outKey, outValue);
	} else {
		for(int i = 0; i < m; i++) {
			outKey.set(i + "," + csv[2]);
			outValue.set("N," + csv[1] + "," + csv[3]);
			context.write(outKey, outValue);
		}
	}		
    }
  }
