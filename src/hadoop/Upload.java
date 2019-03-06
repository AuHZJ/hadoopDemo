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
	
	//上传
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//获取
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		//获得程序到hdfs的输出流
		FSDataOutputStream out = fs.create(new Path(conf.get("outpath")));
		FileSystem localfs = FileSystem.getLocal(conf);
		//获得本地到程序的输入流
		FSDataInputStream in = localfs.open(new Path(conf.get("inpath")));
		IOUtils.copyBytes(in, out, 128, true);
		return 0;
	}

}
