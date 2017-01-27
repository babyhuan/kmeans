package reservori.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

public class ReservoriMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private static int k = 3;
	private static List<Text> result = new ArrayList<>();
	private static int times = 0;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		k = context.getConfiguration().getInt("k", 3);
	}
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		times += 1;
		if(times <= k) result.add(new Text(ivalue));
		else{
			int num = Utils.RandTimes(times);
			if(num < k) result.set(num, new Text(ivalue));
		}
	}
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		for(int i=0; i < result.size();i++){
			context.write(result.get(i), NullWritable.get());
		}
	}
	

}
