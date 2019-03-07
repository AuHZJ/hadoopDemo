package join.mapside;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*预处理程序，将原始数据处理成可被Map端连接的数据
 * 1 分区个数要一致
 * 2 局部排序
 * 3 处理以后的数据不可切割
 */
public class FirstStage {
	public static class SortByKeyMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split(",");
			//获得连接键
			String k = infos[0];
			StringBuffer sb = new StringBuffer();
			sb.append(k);
			for (int i = 1; i < infos.length; i++) {
				sb.append(",");
				sb.append(infos[i]);
			}
			context.write(new Text(k), new Text(sb.toString()));
		}
	}
	public static class SortByKeyReducer extends Reducer<Text, Text, NullWritable, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}
	
}
