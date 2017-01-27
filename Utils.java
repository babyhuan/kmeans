package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	// 集群参数设置
	static String nameNode = "master";
	static String resourceNode = "master";
	static String schedulerNode = "master";
	static String jobHistoryNode = "master";
	private static Configuration conf = null;
	private static FileSystem fs = null;
	/***
	 * 获得配置的Configration
	 * @return conf
	 */
	public static Configuration getConf(){
		if(conf == null){
			conf = new Configuration();
			conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
			conf.set("fs.defaultFS", "hdfs://"+nameNode+":8020");// 指定namenode
			conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
			conf.set("yarn.resourcemanager.address", resourceNode+":8032"); // 指定resourcemanager
			conf.set("yarn.resourcemanager.scheduler.address", schedulerNode+":8030");// 指定资源分配器
			conf.set("mapreduce.jobhistory.address", jobHistoryNode+":10020");// 指定historyserver
			conf.set("mapreduce.job.jar", JarUtil.jar(Utils.class));
		}
		return conf;
	}
	/***
	 * 取得FileSystem连接
	 * @return
	 */
	public static FileSystem getFs(){
		if(fs == null){
			try {
				fs = FileSystem.get(getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fs;
	}
	/***
	 * 验证hdfs上hdfsPath是否存在，如果存在则删除
	 * @param hdfsPath
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static boolean delete(String hdfsPath) throws IllegalArgumentException, IOException{
		return getFs().delete(new Path(hdfsPath), true);
	}
	/***
	 * 计算当前传入mapper中的这行数据到聚类中心向量某一行数据的距离
	 * @param line 当前传入mapper的一行数据
	 * @param string 聚类中心的某一行数据
	 * @param splitter 数据的分隔符
	 * @return distance
	 */
	public static double calDistance(String line, String string, String splitter) {
		double sum = 0.0;
		String[] data = line.split(splitter);
		String[] centerI = string.split(splitter);
		for(int i=0;i<data.length;i++){
			sum += Math.pow(Double.valueOf(data[i]) - Double.valueOf(centerI[i]), 2);
		}
		return Math.sqrt(sum);
	}
	/***
	 * 
	 * @param time
	 * @return [0,time]之间的一个随机整数
	 */
	public static Random random = new Random();
	public static int RandTimes(int time) {
		return random.nextInt(time);
	}
	/***
	 * 判断是否满足误差阈值而决定是否继续更新聚类中心向量
	 * @param preCenter
	 * @param nextCenter
	 * @param delta
	 * @param splitter
	 * @return
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static boolean shouldRunNextIteration(String preCenter, String nextCenter, double delta, String splitter) throws IllegalArgumentException, IOException {
		FSDataInputStream is1 = Utils.getFs().open(new Path(preCenter));
		FSDataInputStream is2 = Utils.getFs().open(new Path(nextCenter));
		BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
		String line1 = "";
		String line2 = "";
		double error = 0.0;
		while((line1 = br1.readLine()) != null){
			line2 = br2.readLine();
			error += calDistance(line1, line2, splitter);
		}
		br1.close();
		br2.close();
		is1.close();
		is2.close();
		if(Math.sqrt(error) < delta){
			System.out.println("delta: "+Math.sqrt(error));
			return false;
		}
		return true;
	}
}
