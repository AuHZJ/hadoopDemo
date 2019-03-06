package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Upload extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Upload(), args);

	}
	
	//�ϴ�
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//��ȡ
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		//��ó���hdfs�������
		FSDataOutputStream out = fs.create(new Path(conf.get("outpath")));
		FileSystem localfs = FileSystem.getLocal(conf);
		//��ñ��ص������������
		FSDataInputStream in = localfs.open(new Path(conf.get("inpath")));
		IOUtils.copyBytes(in, out, 128, true);
		return 0;
	}

}
