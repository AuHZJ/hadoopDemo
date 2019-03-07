package join.mapside;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*Ԥ������򣬽�ԭʼ���ݴ���ɿɱ�Map�����ӵ�����
 * 1 ��������Ҫһ��
 * 2 �ֲ�����
 * 3 �����Ժ�����ݲ����и�
 */
public class FirstStage {
	public static class SortByKeyMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split(",");
			//������Ӽ�
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
