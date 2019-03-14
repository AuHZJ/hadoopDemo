package input;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import mapreduce.YearStation;

public class YearStationRecordReader extends RecordReader<YearStation, DoubleWritable>{
	private LineRecordReader reader = new LineRecordReader();
	private YearStation ys;
	private DoubleWritable tmp_w;
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public YearStation getCurrentKey() throws IOException, InterruptedException {
		return ys;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
		return tmp_w;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!reader.nextKeyValue()) {
			return false;
		}
		//获取当前一整行
		Text value = reader.getCurrentValue();
		String line = value.toString();
		String stationid = line.substring(0, 15);
		String year = line.substring(15, 19);
		String tmp = line.substring(87, 92);
		String qua = line.substring(92, 93);
		// 选取有效数据进行封装
		if (qua.matches("[01459]") && !tmp.equals("9999")) {
			ys = new YearStation(year, stationid);
			tmp_w = new DoubleWritable(Double.parseDouble(tmp));
		}else {
			this.nextKeyValue();
		}
		return true;
	}

}
