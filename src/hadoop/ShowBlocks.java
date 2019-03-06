package hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShowBlocks  extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new ShowBlocks(), args);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//Configuration conf = getConf();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.82:9000");
		FileSystem fs = FileSystem.get(conf);
		//FSDataInputStream in = fs.open(new Path(conf.get("outpath")));
		//FSDataInputStream in = fs.open(new Path("/test.tar.gz"));
		FSDataInputStream in = fs.open(new Path("/hadoop.tar"));
		HdfsDataInputStream hin = (HdfsDataInputStream)in;
		List<LocatedBlock> blocks = hin.getAllBlocks();
		for(LocatedBlock block : blocks) {
			System.out.println("---------------");
			ExtendedBlock b = block.getBlock();
			System.out.println("块名：" + b.getBlockName());
			System.out.println("块id：" + b.getBlockId());
			System.out.println("块大小：" + block.getBlockSize());
			DatanodeInfo[] ls = block.getLocations();
			for(DatanodeInfo l : ls)
				System.out.println("主机名：" + l.getHostName());
		}
		return 0;
	}

}
