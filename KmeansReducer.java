package mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<IntWritable, MyWritable, Text, NullWritable> {
	private String splitter = "";
	private int k = 3;
	private String[] centerData = null;
	@Override
	protected void setup(Reducer<IntWritable, MyWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		splitter = context.getConfiguration().get("splitter");
		k = context.getConfiguration().getInt("k", 3);
		centerData = new String[k];
	}
	public void reduce(IntWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
		double[] sum = null;
		long num = 0;
		for (MyWritable value : values) {
			int number = value.getNum();
			String[] valStr = value.getData().split(splitter);
			if(sum == null){
				sum = new double[valStr.length]; // 初始化sum
				addToSum(sum, valStr);
			}else{
				addToSum(sum, valStr); // 对应字段相加
			}
			num += number;
		}
		averageSum(sum, num); // 对sum中每个元素求平均
		centerData[key.get()] = format(sum);
	}
	private Text result = new Text();
	@Override
	protected void cleanup(Reducer<IntWritable, MyWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		for(int i=0; i < centerData.length;i++){
			result.set(centerData[i]);
			context.write(result, NullWritable.get());
		}
	}
	/***
	 * 格式化数组
	 * 数组元素之间的分隔符采用splitter
	 * @param sum
	 * @return
	 */
	private String format(double[] sum) {
		String str = "";
		for(int i=0; i < sum.length;i++){
			if(i == 0){
				str = str.concat(String.valueOf(sum[i]));
			}else {
				str = str.concat(splitter + String.valueOf(sum[i]));
			}
		}
		return str;
	}
	/***
	 * 对数组上的每一个元素求平均值
	 * @param sum
	 * @param num
	 */
	private void averageSum(double[] sum, long num) {
		for(int i=0;i < sum.length;i++){
			sum[i] = sum[i]/num;
		}
	}
	/***
	 * 两个数组对应字段相加
	 * @param sum
	 * @param valStr
	 */
	private void addToSum(double[] sum, String[] valStr) {
		for(int i=0;i < sum.length;i++){
			sum[i] += Double.valueOf(valStr[i]);
		}
	}

}
