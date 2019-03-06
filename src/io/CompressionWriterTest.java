package io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//hadoop jar test.jar io/CompressionWriterTest -D inpath=test.jar -D outpath=/jar.gz


public class CompressionWriterTest extends Configured implements Tool  {

	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		ToolRunner.run(new CompressionWriterTest(), args);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		FileSystem hfs = FileSystem.get(conf);
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		//java --> hdfs
		FSDataOutputStream hout = hfs.create(new Path(conf.get("outpath")));
		//local --> java
		FSDataInputStream lin = lfs.open(new Path(conf.get("inpath")));
		//增加压缩功能
		//利用CompressCodec包装 hout为带有压缩功能输出流
		//利用工厂模式可以获得CompressCodec
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		//为何要传输出路径？（用文件后缀名通知codec压缩算法是什么）
		CompressionCodec codec = factory.getCodec(new Path(conf.get("outpath")));
		CompressionOutputStream chout = codec.createOutputStream(hout);
		//上传
		IOUtils.copyBytes(lin, chout, 128, true);
		return 0;
	}

}
