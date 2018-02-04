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

import java.util.HashMap;

  public class MatrixReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	String[] value;

	HashMap<Integer, Integer> a = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> b = new HashMap<Integer, Integer>();
	
	for (Text val: values) {
		System.out.print(val);
		value = val.toString().split(",");
		if(value[0].equals("M")) {
			a.put(Integer.parseInt(value[1]), 
				Integer.parseInt(value[2]));
			
		} else {
			b.put(Integer.parseInt(value[1]), 
				Integer.parseInt(value[2]));
		}
	}
	int n = Integer.parseInt(context.getConfiguration().get("n"));
	int result = 0;
	int mij;
	int njk;
	for(int i = 0; i < n; i++) {
		mij = a.containsKey(i) ? a.get(i) : 0;
		njk = b.containsKey(i) ? b.get(i) : 0;
		result += mij * njk;
	}
	if (result != 0) {
		context.write(null, 
			new Text(key.toString() + "," + Integer.toString(result)));
	}
    }
  }
