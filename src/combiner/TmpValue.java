package combiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TmpValue implements Writable {
	private DoubleWritable tmp = new DoubleWritable();
	private IntWritable num = new IntWritable();

	public TmpValue() {
	}

	public TmpValue(DoubleWritable tmp, IntWritable num) {
		this.tmp = new DoubleWritable(tmp.get());
		this.num = new IntWritable(num.get());
	}

	public TmpValue(double tmp, int num) {
		this.tmp = new DoubleWritable(tmp);
		this.num = new IntWritable(num);
	}

	public DoubleWritable getTmp() {
		return tmp;
	}

	public void setTmp(DoubleWritable tmp) {
		this.tmp = new DoubleWritable(tmp.get());
	}

	public IntWritable getNum() {
		return num;
	}

	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tmp.readFields(in);
		num.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tmp.write(out);
		num.write(out);
	}

	@Override
	public String toString() {
		return tmp.get() + "\t" + num.get();
	}

}
