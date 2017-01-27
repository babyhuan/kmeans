package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyWritable implements Writable{
	private int num = 1;
	private String data = "";
	public MyWritable() {}
	public MyWritable(int num, String data){
		this.num = num;
		this.data = data;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		num = in.readInt();
		data = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(num);
		out.writeUTF(data);
	}

}
