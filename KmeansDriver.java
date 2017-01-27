package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import mr.KmeansMapper;
import mr.KmeansReducer;
import mr.MyWritable;
import util.Utils;

public class KmeansDriver {

	public static void main(String[] args) throws Exception {
		if(args.length < 5){
			System.err.println("Usage: driver.KmeansDriver <output> "
					+ "<splitter> <k> <centerPath> <input1> [<input2>..]");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf = Utils.getConf();
		String output = args[0];
		String splitter = args[1];
		int k = Integer.valueOf(args[2]);
		String centerPath = args[3];
		conf.set("splitter", splitter);
		conf.set("centerPath", centerPath);
		conf.setInt("k", k);
		Job job = Job.getInstance(conf, "kmeans center path:"+centerPath+",output"+output);
//		job.setJarByClass(driver.KmeansDriver.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MyWritable.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 检测输出路径是否存在，如果存在则删除
		Utils.delete(output);
		// 格式化所有输入路径的文件
		for(int i=4; i < args.length;i++){
			FileInputFormat.setInputPaths(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(output));

		if (!job.waitForCompletion(true))
			return;
	}


}
