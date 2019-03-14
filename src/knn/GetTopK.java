package knn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetTopK extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetTopK(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "GetTopK");
		job.setJarByClass(GetTopK.class);
		job.setMapperClass(DMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(DReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path("/knn_data/gsd_result_sorted/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/gsd_result_sorted_topk"));
		job.waitForCompletion(true);
		return 0;
	}

	public static class DMapper extends Mapper<LongWritable, Text, Text, Text> {
		private int count = 0;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (count == 20) {
				return;
			}
			count++;
			String line = value.toString();
			String[] infos = line.split("\t");
			String tag = infos[0].substring(0, infos[0].indexOf("_"));
			String degree = infos[1];
			context.write(new Text(tag), new Text(degree));
		}
	}

	public static class DReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			double avgDegree = 0.0;
			for (Text value : values) {
				count++;
				avgDegree += Double.parseDouble(value.toString());
			}
			context.write(key, new Text(count + "\t" + avgDegree / count));
		}
	}

}
