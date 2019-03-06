package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSortFirstStage extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TotalSortFirstStage(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "TotalSortFirstStage");
		job.setJarByClass(TotalSortFirstStage.class);

		job.setMapperClass(PartialSort.PSMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(PartialSort.PSReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
