package sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSort extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TotalSort(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(conf.get("inpath"));
		Path out = new Path(conf.get("outpath"));

		Job job = Job.getInstance(conf, "TotalSort");
		job.setJarByClass(TotalSort.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, in);
		TextOutputFormat.setOutputPath(job, out);

		// ----------------------
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setNumReduceTasks(2);
		RandomSampler<DoubleWritable, Text> sampler = new InputSampler.RandomSampler<DoubleWritable, Text>(0.5, 200, 5);

		// ���в������ݣ�����������ݲ�����
		InputSampler.writePartitionFile(job, sampler);

		// �ѱ��浽���صķ����ļ����ַ���job�ĸ����ڵ�
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI uri = new URI(partitionFile);
		job.addCacheFile(uri);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
