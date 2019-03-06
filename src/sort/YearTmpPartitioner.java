package sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//分区比较器，分区标准只参考自然键 年份
//泛型代表map阶段输出数据的类型
public class YearTmpPartitioner extends Partitioner<YearTmp, Text> {

	@Override
	public int getPartition(YearTmp key, Text value, int numPartitions) {
		return Math.abs(key.getYear().get() % numPartitions);
	}

}
