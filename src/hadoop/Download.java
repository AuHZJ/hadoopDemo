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

public class Download extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Download(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(conf.get("inpath")));
		FileSystem lfs = FileSystem.getLocal(conf);
		FSDataOutputStream out = lfs.create(new Path(conf.get("outpath")));
		IOUtils.copyBytes(in, out, 128, true);
		return 0;
	}

}
