package driver;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import classify.ClassifyPartition;
import classify.ClassifyReducer;
import mr.KmeansMapper;
import mr.MyWritable;
import reservori.driver.ReservoriDriver;
import util.Utils;

public class AllJob {
	public static void main(String[] args) throws Exception {
		String[] KmeansArgs = new String[]{
			"hdfs://master:8020/user/wmh/kmeans/data.txt", // 原始数据
			"hdfs://master:8020/user/wmh/kmeans/classify", // 按照类别分类的数据
			"3", // 聚类个数
			",", // 分隔符
			"20", // 迭代次数
			"0.5", // 误差阈值
			"0" // start:0 -> 初始化聚类中心开始；1 ->计算新的聚类中心开始；2 -> 分类
		};
		String input = KmeansArgs[0];
		String output = KmeansArgs[1];
		int k = Integer.valueOf(KmeansArgs[2]);
		String splitter = KmeansArgs[3];
		int iteration = Integer.valueOf(KmeansArgs[4]);
		double delta = Double.valueOf(KmeansArgs[5]);
		int start = Integer.valueOf(KmeansArgs[6]);
		int number = 0;
		// 根据start的值来执行相应的任务
		int ret = -1;
		String fileStr = "iter";
		switch (start) {
		case 0: first(k,output,input,ret);
		case 1:	number = updateKmeans(input, output, k, splitter, 
				delta, ret, iteration);
		case 2: if(start == 2){
				number = readLastFile(output,fileStr) - 1;
				}
				classify(number, input, output, k, splitter, iteration);
		default: break;
		}
		
	}
	/***
	 * 对数据按照最终聚类中心聚类，并按照类别输出
	 * @param number
	 * @param input
	 * @param output
	 * @param k
	 * @param splitter
	 * @param iteration
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void classify(int number, String input, String output, int k, String splitter, int iteration) throws IOException, ClassNotFoundException, InterruptedException {
		if(number == 0){
			number = iteration;
		}
		Configuration conf = Utils.getConf();
		conf.set("splitter", splitter);
		conf.set("centerPath", output+"/iter"+number+"/part-r-00000");
		conf.setInt("k", k);
		Job job = Job.getInstance(conf, "classify");
		job.setMapperClass(KmeansMapper.class);
		job.setPartitionerClass(ClassifyPartition.class);
		job.setReducerClass(ClassifyReducer.class);
		job.setNumReduceTasks(k);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MyWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		Path out = new Path(output + "/clustered");
		FileOutputFormat.setOutputPath(job, out);
		if(Utils.getFs().exists(out)){
			Utils.getFs().delete(out,true);
		}
		System.exit(job.waitForCompletion(true)?0:1);
	}
	/***
	 * 读取之前生成的文件个数，返回最终聚类中心文件名中的下标+1
	 * @param output
	 * @param fileStr
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static int readLastFile(String output, String fileStr) throws FileNotFoundException, IOException {
		Path path = new Path(output);
		FileStatus[] fs = Utils.getFs().listStatus(path);
		int num = 0;
		for(int i=0;i<fs.length;i++){
			if(fs[i].getPath().getName().startsWith(fileStr)){
				num++;
			}
		}
		return num;
	}
	/***
	 * 循环更新聚类中心向量
	 * @param input
	 * @param output
	 * @param k
	 * @param splitter
	 * @param delta
	 * @param ret
	 * @param iteration
	 * @return
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	private static int updateKmeans(String input, String output, int k, String splitter, double delta, int ret,
			int iteration) throws IllegalArgumentException, IOException {
		int num = 0;
		for(int i=0; i < iteration;i++){
			String[] job2Args = new String[]{
					output + "/iter" + (i+1), //当前聚类中心
					splitter,
					String.valueOf(k),
					output+ "/iter" + i + "/part-r-00000", // 上一次聚类中心
					input
			};
			try {
				KmeansDriver.main(job2Args);
			} catch (Exception e) {
				System.err.println("kmeans job failed!"+":"+i);
				System.exit(-1);
			}
			if(!Utils.shouldRunNextIteration(output+"/iter"+i+"/part-r-00000",
					output+"/iter"+(i+1)+"/part-r-00000",delta,splitter)){
				num = i + 1;
				break;
			}
		}
		return num;
	}
	/***
	 * 调用蓄水池抽样算法，初始化聚类中心向量
	 * @param k
	 * @param output
	 * @param input
	 * @param ret
	 * @throws Exception 
	 */
	private static void first(int k, String output, String input, int ret) throws Exception {
		String[] job1Args = new String[]{
				String.valueOf(k),
				output+"/iter0",
				input
		};
		ret = ToolRunner.run(Utils.getConf(), new ReservoriDriver(),job1Args);
		if(ret != 0){
			System.err.println("job 1 failed");
			System.exit(-1);
		}
	}
}
