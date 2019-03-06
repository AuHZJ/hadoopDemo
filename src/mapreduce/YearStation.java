package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearStation implements WritableComparable<YearStation> {

	private IntWritable year = new IntWritable();
	private Text stationId = new Text();

	public YearStation() {
		super();
		// TODO Auto-generated constructor stub
	}

	public YearStation(String year, String stationId) {
		super();
		int y = Integer.parseInt(year);
		this.year = new IntWritable(y);
		this.stationId = new Text(stationId);
	}

	public YearStation(IntWritable year, Text stationId) {
		this.year = new IntWritable(year.get());
		this.stationId = new Text(stationId.toString());
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = new IntWritable(year.get());
	}

	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = new Text(stationId.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.year.readFields(in);
		this.stationId.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.year.write(out);
		this.stationId.write(out);
	}

	@Override
	public int compareTo(YearStation o) {
		// TODO Auto-generated method stub
		return (this.year.get() - o.year.get()) == 0 ? this.stationId.compareTo(o.stationId)
				: (this.year.get() - o.year.get());
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.year.get()+"\t"+this.stationId.toString();
	}
}
