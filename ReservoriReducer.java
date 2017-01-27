package reservori.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Utils;

public class ReservoriReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	private static int k = 3;
	private static List<Text> result = new ArrayList<>();
	@Override
	protected void setup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		k = context.getConfiguration().getInt("k", 3);
	}
	private static int times = 0;
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		times += 1;
		if(times <= k) result.add(new Text(key));
		else{
			int num = Utils.RandTimes(times);
			if(num < k) result.set(num, new Text(key));
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		for(int i=0;i < result.size();i++){
			context.write(result.get(i), NullWritable.get());
		}
	}

}
