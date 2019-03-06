package sort;

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

public class SecondarySort extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SecondarySort(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "SecondarySort");
		job.setJarByClass(SecondarySort.class);

		job.setMapperClass(SSMapper.class);
		job.setMapOutputKeyClass(YearTmp.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SSReducer.class);
		job.setOutputKeyClass(YearTmp.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		// 显式设置分区比较器
		job.setPartitionerClass(YearTmpPartitioner.class);
		// 显式设置分组比较器
		job.setGroupingComparatorClass(YearTmpGroupComparator.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class SSMapper extends Mapper<LongWritable, Text, YearTmp, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, YearTmp, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split("\t");
			YearTmp yt = new YearTmp(Integer.parseInt(infos[0]), Double.parseDouble(infos[2]));
			context.write(yt, new Text(infos[1]));
			// System.out.println(infos[0]+","+infos[1]+","+infos[2]);
		}
	}

	public static class SSReducer extends Reducer<YearTmp, Text, YearTmp, Text> {
		@Override
		protected void reduce(YearTmp key, Iterable<Text> values, Reducer<YearTmp, Text, YearTmp, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				System.out.println("=====" + key + " " + value + "=====");
				context.write(key, value);
			}
		}
	}
}
