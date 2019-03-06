package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class GetConcurrence extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetConcurrence(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "GetConcurrence");
		job.setJarByClass(GetConcurrence.class);

		job.setMapperClass(GCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(GCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static class GCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] names = line.split(",");
			for(int i = 1; i < names.length-1; i++) {
				for(int j = i+1; j < names.length; j++) {
					if(names[i].compareTo(names[j]) > 0) {
						context.write(new Text(names[i]+","+names[j]), new IntWritable(1));
					}else {
						context.write(new Text(names[j]+","+names[i]), new IntWritable(1));
					}
					
				}
			}
		}
	}

	public static class GCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
