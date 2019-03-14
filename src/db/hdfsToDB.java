package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class hdfsToDB extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new hdfsToDB(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "hdfsToDB");
		job.setJarByClass(hdfsToDB.class);

		job.setMapperClass(MaxTmpMapper.class);
		job.setMapOutputKeyClass(YearStat.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setReducerClass(MaxTmpReducer.class);
		job.setOutputKeyClass(YearStat.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		
		DBConfiguration.configureDB(job.getConfiguration(), "oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@192.168.43.23:1521:XE", "hadoop", "1998");
//		DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver",
//				"jdbc:mysql://192.168.1.101:3306/hadoop", "root", "123456789");
		DBOutputFormat.setOutput(job, "maxtmp", "year", "stationid", "tmp");
		job.setOutputFormatClass(DBOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MaxTmpMapper extends Mapper<LongWritable, Text, YearStat, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, YearStat, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String stationid = line.substring(0, 15);
			String year = line.substring(15, 19);
			String tmp = line.substring(87, 92);
			String qua = line.substring(92, 93);
			// 选取有效数据进行封装
			if (qua.matches("[01459]") && !tmp.equals("9999")) {
				YearStat ys = new YearStat(year, stationid, "0.0");
				DoubleWritable tmp_w = new DoubleWritable(Double.parseDouble(tmp));
				// 输出数据
				context.write(ys, tmp_w);
			}
		}
	}

	public static class MaxTmpReducer extends Reducer<YearStat, DoubleWritable, YearStat, NullWritable> {
		@Override
		protected void reduce(YearStat key, Iterable<DoubleWritable> values,
				Reducer<YearStat, DoubleWritable, YearStat, NullWritable>.Context context)
				throws IOException, InterruptedException {
			double max = 0.0;
			for (DoubleWritable value : values) {
				max = Math.max(max, value.get());
			}
			key.setTmp(new DoubleWritable(max));
			context.write(key, NullWritable.get());
		}
	}
}
