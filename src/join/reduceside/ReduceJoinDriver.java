package join.reduceside;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoinDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ReduceJoinDriver(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		return 0;

	}

	public static class RJoinMapper1 extends Mapper<LongWritable, Text, IdTag, Text> {
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IdTag, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String line = value.toString();
			String infos[] = line.split(",");
			String k = infos[0];
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < infos.length; i++) {
				sb.append(",");
				sb.append(infos[i]);
			}
			context.write(new IdTag(k, 0), new Text(sb.toString()));
		}
	}
	
	public static class RJoinMapper2 extends Mapper<LongWritable, Text, IdTag, Text> {
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IdTag, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String line = value.toString();
			String infos[] = line.split(",");
			String k = infos[0];
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < infos.length; i++) {
				sb.append(",");
				sb.append(infos[i]);
			}
			context.write(new IdTag(k, 1), new Text(sb.toString()));
		}
	}
	
	public static class RJoinReducer extends Reducer<IdTag, Text, NullWritable, Text>{
		@Override
		protected void reduce(IdTag key, Iterable<Text> values, Reducer<IdTag, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			sb.append(key.getId().toString());
			for (Text value : values) {
				sb.append(value.toString());
			}
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}

}
