package sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//�����Ƚ�����������׼ֻ�ο���Ȼ�� ���
//���ʹ���map�׶�������ݵ�����
public class YearTmpPartitioner extends Partitioner<YearTmp, Text> {

	@Override
	public int getPartition(YearTmp key, Text value, int numPartitions) {
		return Math.abs(key.getYear().get() % numPartitions);
	}

}
