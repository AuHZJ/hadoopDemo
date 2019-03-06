package combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapreduce.YearStation;

public class AvgTmpByYSCombiner extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AvgTmpByYSCombiner(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "avg_combiner");
		job.setJarByClass(this.getClass());

		job.setMapperClass(ATBYSMapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass(TmpValue.class);

		job.setCombinerClass(ATBYSCombiner.class);

		job.setReducerClass(ATBYSReducer.class);
		job.setOutputKeyClass(YearStation.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ATBYSMapper extends Mapper<LongWritable, Text, YearStation, TmpValue> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, YearStation, TmpValue>.Context context)
				throws IOException, InterruptedException {
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
				TmpValue tv = new TmpValue(tmp_w, new IntWritable(1));
				// 输出数据
				context.write(ys, tv);
			}
		}
	}

	public static class ATBYSCombiner extends Reducer<YearStation, TmpValue, YearStation, TmpValue> {
		@Override
		protected void reduce(YearStation key, Iterable<TmpValue> values,
				Reducer<YearStation, TmpValue, YearStation, TmpValue>.Context context)
				throws IOException, InterruptedException {
			// 接收新的权重值
			int num = 0;
			// 接收新的温度值
			double tmp_sum = 0.0;
			for (TmpValue value : values) {
				tmp_sum += value.getTmp().get() * value.getNum().get();
				num += value.getNum().get();
			}
			double new_avg = tmp_sum / num;
			// 计算过一次加权平均的符合类型
			TmpValue tv = new TmpValue(new_avg, num);
			context.write(key, tv);
		}
	}

	public static class ATBYSReducer extends Reducer<YearStation, TmpValue, YearStation, DoubleWritable> {
		@Override
		protected void reduce(YearStation key, Iterable<TmpValue> values,
				Reducer<YearStation, TmpValue, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			int num = 0;
			double tmp_sum = 0.0;
			for (TmpValue value : values) {
				tmp_sum += value.getTmp().get() * value.getNum().get();
				num += value.getNum().get();
			}
			double avg_tmp = tmp_sum / num;
			context.write(key, new DoubleWritable(avg_tmp));
		}
	}

}
