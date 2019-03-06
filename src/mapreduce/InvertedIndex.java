package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new InvertedIndex(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(indexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(indexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class indexMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//��ȡ�ؼ��ʣ��ļ���
			//�ļ���
			FileSplit split = (FileSplit)context.getInputSplit();
			String name = split.getPath().getName();
			//������ģʽOOAD��StringTokenizerЧ����split��ͬ
			StringTokenizer token = new StringTokenizer(value.toString(), " ");
			String item = null;
			while(token.hasMoreTokens()) {
				item = token.nextToken();
				if(item.trim().length() >= 1) {
					context.write(new Text(item.trim()), new Text(name.trim()));
				}
			}
		}
	}
	
	public static class indexReduce extends Reducer<Text, Text, Text, Text>{
		private HashMap<String, Integer> count = new HashMap<>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//���õ����ļ����ŵ�map��
			//key = �ļ�����value = ����
			//������õ����ļ�������map������ͬ��key��������Ѿ�ͳ�ƹ�������value+1
			//������õ����ļ�������map��û������ͬ��key�������û��ͳ�ƹ�������key = name�� value = 1
			count.clear();
			for (Text value : values) {
				String name = value.toString();
				if(count.containsKey(name)) {
					count.put(name, count.get(name)+1);
				}else {
					count.put(name, 1);
				}
			}
			StringBuffer str = new StringBuffer();
			for(Entry<String, Integer> e : count.entrySet()) {
				str.append(","+e.getKey()+":"+e.getValue());
			}
			String v_line = str.toString();
			context.write(key, new Text(v_line));
		}
	}

}
