package knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagAvgNum implements Writable {
	private Text tag = new Text();
	private DoubleWritable avg = new DoubleWritable();
	private IntWritable num = new IntWritable();

	public TagAvgNum() {
	}

	public TagAvgNum(Text line) {
		String[] infos = line.toString().split("\t");
		this.tag = new Text(infos[0]);
		this.avg = new DoubleWritable(Double.parseDouble(infos[2]));
		this.num = new IntWritable(Integer.parseInt(infos[1]));
	}

	public TagAvgNum(String tag, double avg, int num) {
		this.tag = new Text(tag);
		this.avg = new DoubleWritable(avg);
		this.num = new IntWritable(num);
	}
	
	public TagAvgNum(TagAvgNum tan) {
		this.tag = new Text(tan.getTag().toString());
		this.avg = new DoubleWritable(tan.getAvg().get());
		this.num = new IntWritable(tan.getNum().get());
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
	}

	public DoubleWritable getAvg() {
		return avg;
	}

	public void setAvg(DoubleWritable avg) {
		this.avg = new DoubleWritable(avg.get());
	}

	public IntWritable getNum() {
		return num;
	}

	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag.readFields(in);
		this.avg.readFields(in);
		this.num.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.tag.write(out);
		this.avg.write(out);
		this.num.write(out);
	}

}
