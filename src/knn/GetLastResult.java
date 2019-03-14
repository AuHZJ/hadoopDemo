package knn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetLastResult extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetLastResult(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "GetLastResult");
		job.setJarByClass(GetLastResult.class);

		job.setMapperClass(GLRMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TagAvgNum.class);

		job.setReducerClass(GLRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path("/knn_data/gsd_result_sorted_topk/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/gsd_result_sorted_topk_lastResult"));

		job.waitForCompletion(true);
		return 0;
	}

	public static class GLRMapper extends Mapper<LongWritable, Text, Text, TagAvgNum> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TagAvgNum>.Context context)
				throws IOException, InterruptedException {
			TagAvgNum tan = new TagAvgNum(value);
			context.write(new Text("a"), tan);
		}
	}

	public static class GLRReducer extends Reducer<Text, TagAvgNum, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<TagAvgNum> values,
				Reducer<Text, TagAvgNum, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			Iterator<TagAvgNum> ite = values.iterator();
			TagAvgNum max = new TagAvgNum(ite.next());
			while (ite.hasNext()) {
				TagAvgNum current = new TagAvgNum(ite.next());
				if (max.getNum().get() < current.getNum().get()) {
					max = new TagAvgNum(current);
				} else if (max.getNum().get() == current.getNum().get()) {
					if (max.getAvg().get() < current.getAvg().get()) {
						max = new TagAvgNum(current);
					}
				}
			}
			context.write(max.getTag(), NullWritable.get());
		}
	}
}
