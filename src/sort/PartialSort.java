package sort;

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

public class PartialSort extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PartialSort(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "PartialSort");
		job.setJarByClass(PartialSort.class);
		
		job.setMapperClass(PSMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PSReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		job.setNumReduceTasks(2);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class PSMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split("\t");
			DoubleWritable tmp = new DoubleWritable(Double.parseDouble(infos[2]));
			context.write(tmp, value);
		}
	}
	
	public static class PSReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,
				Reducer<DoubleWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

}
