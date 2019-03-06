package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgTmpByYearStation extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AvgTmpByYearStation(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "AvgTmpByYearStation");
		job.setJarByClass(AvgTmpByYearStation.class);

		job.setMapperClass(AvgMapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(YearStation.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AvgMapper extends Mapper<LongWritable, Text, YearStation, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// 提取字段 Year StationId tmp 数据质量
			String line = value.toString();
			String stationid = line.substring(0, 15);
			String year = line.substring(15, 19);
			String tmp = line.substring(87, 92);
			String qua = line.substring(92, 93);
			// 选取有效数据进行封装
			if (qua.matches("[01459]") && !tmp.equals("9999")) {
				YearStation ys = new YearStation(year, stationid);
				double tmp_d = Double.parseDouble(tmp);
				DoubleWritable tmp_w = new DoubleWritable(tmp_d);
				// 输出数据
				context.write(ys, tmp_w);
			}
		}
	}

	public static class AvgReducer extends Reducer<YearStation, DoubleWritable, YearStation, DoubleWritable> {
		@Override
		protected void reduce(YearStation key, Iterable<DoubleWritable> values,
				Reducer<YearStation, DoubleWritable, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int num = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
				num++;
			}
			double avgTmp = sum / num;
			context.write(key, new DoubleWritable(avgTmp));
		}
	}

}
