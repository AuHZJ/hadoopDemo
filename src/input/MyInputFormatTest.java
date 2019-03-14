package input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapreduce.YearStation;

public class MyInputFormatTest extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MyInputFormatTest(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "MyInputFormatTest");
		job.setJarByClass(MyInputFormatTest.class);
		
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(MaxTmpReducer.class);
		job.setOutputKeyClass(YearStation.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		return 0;
	}
	public static class MaxTmpReducer extends Reducer<YearStation, DoubleWritable, YearStation, DoubleWritable> {
		@Override
		protected void reduce(YearStation key, Iterable<DoubleWritable> values,
				Reducer<YearStation, DoubleWritable, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double max = 0.0;
			for (DoubleWritable value : values) {
				max = Math.max(max, value.get());
			}
			context.write(key, new DoubleWritable(max));
		}
	}

}
