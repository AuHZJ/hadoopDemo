package knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetSimilarityDegree extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetSimilarityDegree(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "GetSimilarityDegree");
		job.setJarByClass(GetSimilarityDegree.class);

		job.setMapperClass(GSDMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path("/knn_data/train_bin"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/gsd_result"));

		job.waitForCompletion(true);
		return 0;
	}

	static char[] unkown = new char[400];

	public static class GSDMapper extends Mapper<Text, Text, Text, DoubleWritable> {
		@Override
		protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// 获取待识别向量 /knn_data/unkown
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("/knn_data/unkown"));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			unkown = reader.readLine().toCharArray();
		}

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// 计算相似度
			char[] train_array = value.toString().toCharArray();
			double sum = 0;
			for (int i = 0; i < train_array.length; i++) {
				// 待识别向量中，下标为i的值，某个维度
				int x = Integer.parseInt(Character.toString(unkown[i]));
				// 训练集某个向量中，下标为i的值，某个维度
				int t = Integer.parseInt(Character.toString(train_array[i]));
				sum += Math.pow((x - t), 2);
//				sum += (x - t) * (x - t);
			}
			double degree = 1 / (1 + Math.sqrt(sum));
			context.write(key, new DoubleWritable(degree));
		}
	}

}
