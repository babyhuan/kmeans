package reservori.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reservori.mr.ReservoriMapper;
import reservori.mr.ReservoriReducer;
import util.JarUtil;

public class ReservoriDriver extends Configured implements Tool {
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new ReservoriDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		int k = Integer.valueOf(arg0[0]); // 获取抽样个数k
		String outputPath = arg0[1]; // 输出文件路径
		Configuration conf = getConf();
		conf = getMyConf(conf);
		conf.setInt("k", k);
		Job job = Job.getInstance(conf, "kmeans reservori");
		job.setMapperClass(ReservoriMapper.class);
		job.setReducerClass(ReservoriReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 设置输出文件格式
		job.setOutputFormatClass(TextOutputFormat.class);
		// 判断输出路径是否存在，如果存在则删除
		Path out = new Path(outputPath);
		out.getFileSystem(conf).delete(out, true);
//		for(int i=2; i < arg0.length;i++){
//			FileInputFormat.setInputPaths(job, new Path(arg0[i]));
//		}
		FileInputFormat.setInputPaths(job, new Path(arg0[2]));
		FileOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true)?0 : 1;
	}
	public static Configuration getMyConf(Configuration conf){
		conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
	    conf.set("fs.defaultFS", "hdfs://"+"master"+":8020");// 指定namenode
	    conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
	    String resourcenode = "master";
	    conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // 指定resourcemanager
	    conf.set("yarn.resourcemanager.scheduler.address", resourcenode+":8030");// 指定资源分配器
	    conf.set("mapreduce.jobhistory.address", resourcenode+":10020");// 指定historyserver
	    conf.set("mapreduce.job.jar", JarUtil.jar(ReservoriDriver.class));// jar path
		return conf;
	 }
}
