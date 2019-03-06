package chain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PatentRefCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PatentRefCount(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(conf.get("inpath"));
		Path out = new Path(conf.get("outpath"));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		Job job = Job.getInstance(conf, "PatentRefCount");
		// job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
		// ",");
		job.setJarByClass(PatentRefCount.class);

		ChainMapper.addMapper(job, InverseMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, MyMapper.class, Text.class, Text.class, Text.class, LongWritable.class, conf);

		ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class,
				LongWritable.class, conf);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, in);
		TextOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new LongWritable(1));
		}
	}

}

