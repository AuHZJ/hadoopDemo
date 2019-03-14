package db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class YearStat implements WritableComparable<YearStat>, DBWritable {

	private IntWritable year = new IntWritable();
	private Text stationid = new Text();
	private DoubleWritable tmp = new DoubleWritable();

	public YearStat() {
	}

	public YearStat(String year, String stationId, String tmp) {
		super();
		int y = Integer.parseInt(year);
		this.year = new IntWritable(y);
		this.stationid = new Text(stationId);
		this.tmp = new DoubleWritable(Double.parseDouble(tmp));
	}

	public YearStat(IntWritable year, Text stationId, DoubleWritable tmp) {
		this.year = new IntWritable(year.get());
		this.stationid = new Text(stationId.toString());
		this.tmp = new DoubleWritable(tmp.get());
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = new IntWritable(year.get());
	}

	public Text getStationId() {
		return stationid;
	}

	public void setStationId(Text stationId) {
		this.stationid = new Text(stationId.toString());
	}

	public DoubleWritable getTmp() {
		return tmp;
	}

	public void setTmp(DoubleWritable tmp) {
		this.tmp = new DoubleWritable(tmp.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.stationid.readFields(in);
		this.tmp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.year.write(out);
		this.stationid.write(out);
		this.tmp.write(out);
	}

	@Override
	public int compareTo(YearStat o) {
		return (this.year.get() - o.year.get()) == 0 ? this.stationid.compareTo(o.stationid)
				: (this.year.get() - o.year.get());
	}

	@Override
	public String toString() {
		return year.get() + " " + stationid.toString() + " " + tmp.get();
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		if (rs == null) {
			return;
		}
		this.year = new IntWritable(rs.getInt(1));
		this.stationid = new Text(rs.getString(2));
		this.tmp = new DoubleWritable(rs.getDouble(3));
	}

	@Override
	public void write(PreparedStatement prep) throws SQLException {
		prep.setInt(1, this.year.get());
		prep.setString(2, this.stationid.toString());
		prep.setDouble(3, this.tmp.get());
	}
}
