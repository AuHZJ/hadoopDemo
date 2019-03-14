package workflow;

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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class PatentRefSort extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PatentRefSort(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(conf.get("inpath"));
		Path out = new Path(conf.get("outpath"));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		Job job = Job.getInstance(conf, "PatentRefSort");
		job.setJarByClass(PatentRefSort.class);
		ChainMapper.addMapper(job, InverseMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, MyMapper.class, Text.class, Text.class, Text.class, LongWritable.class, conf);
		ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class,
				LongWritable.class, conf);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, in);
		SequenceFileOutputFormat.setOutputPath(job, out);
		// -------------
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(PatentRefSort.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job2, out);
		ChainMapper.addMapper(job2, InverseMapper.class, Text.class, LongWritable.class, LongWritable.class, Text.class,
				job2.getConfiguration());
		ChainReducer.setReducer(job2, Reducer.class, LongWritable.class, Text.class, LongWritable.class, Text.class,
				job2.getConfiguration());
		ChainMapper.addMapper(job2, InverseMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class,
				job2.getConfiguration());
		job2.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job2, new Path("/prs_result"));
		//---------------
		ControlledJob cjob1 = new ControlledJob(conf);
		cjob1.setJob(job);
		ControlledJob cjob2 = new ControlledJob(conf);
		cjob2.setJob(job2);
		cjob2.addDependingJob(cjob1);
		JobControl control = new JobControl("prs");
		control.addJob(cjob1);
		control.addJob(cjob2);
		Thread thread =new Thread(control);
		thread.start();
		while(true) {
			for(ControlledJob j: control.getRunningJobList()) {
				j.getJob().monitorAndPrintJob();
			}
			if(control.allFinished()) {
				break;
			}
		}
		return 0;
	}

	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new LongWritable(1));
		}
	}

}
