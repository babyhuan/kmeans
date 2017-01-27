package classify;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import mr.MyWritable;


public class ClassifyPartition extends Partitioner<IntWritable, MyWritable>{

	@Override
	public int getPartition(IntWritable key, MyWritable value, int k) {
		if(key.get() == 0)return 0;
		else if (key.get() == 1) return 1;
		else return 2;
	}

}
