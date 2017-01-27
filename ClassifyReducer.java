package classify;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mr.MyWritable;

public class ClassifyReducer extends Reducer<IntWritable, MyWritable, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
		
		for (MyWritable val : values) {
			context.write(key, new Text(val.getData()));
		}
	}

}
