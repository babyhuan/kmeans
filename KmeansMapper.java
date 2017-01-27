package mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.Utils;

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, MyWritable> {
	private String splitter = ""; // 分隔符
	private String centerPath = ""; // 聚类中心路径
	private int k = 3; // 聚类中心个数
	private String[] centerData = null; // 聚类中心的数组
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		k = context.getConfiguration().getInt("k", 3);
		splitter = context.getConfiguration().get("splitter");
		centerPath = context.getConfiguration().get("centerPath");
		centerData = new String[k];
		// 利用java API从hdfs上读取聚类中心的数据并保存在centerData中
		FSDataInputStream iStream = Utils.getFs().open(new Path(centerPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(iStream));
		String line = "";
		int i = 0;
		while((line = br.readLine()) != null){
			centerData[i++] = line;
		}
		br.close();
		iStream.close();
	}
	private IntWritable ID = new IntWritable();
	private MyWritable mw = new MyWritable();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int vecId = getCenterId(value.toString());
		ID.set(vecId);
		mw.setData(value.toString());
		context.write(ID, mw);
	}
	/***
	 * 计算当前行数据到聚类中心向量中距离最小的下标
	 * @param line
	 * @return 下标 [0,k)
	 */
	private int getCenterId(String line) {
		int type = -1;
		double max = Double.MAX_VALUE;
		double distance = 0.0;
		for(int j=0; j<centerData.length;j++){
			distance = Utils.calDistance(line,centerData[j],splitter);
			if(distance < max){
				max = distance;
				type = j;
			}
		}
		return type;
	}

}
