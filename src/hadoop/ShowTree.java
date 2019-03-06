package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShowTree extends Configured implements Tool{
	
	FileSystem fs = null;
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new ShowTree(), args);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		fs = FileSystem.get(conf);
		//拿到某个路径下的所有子目录和子文件的元数据
		FileStatus[] list = fs.listStatus(new Path(conf.get("path")));
		for(FileStatus sta : list)
			show(sta);
		return 0;
	}
	
	public void show(FileStatus sta) {
		if(sta.isFile() && sta.getLen() > 0) {
			showDetail(sta);
		}else if(sta.isDirectory()){
			//获得到该目录下的直接一级文件的元数据
			//继续对每个元数据调用show
			try {
				//Stream.of(fs.listStatus(sta.getPath())).forEach(this::show);
				FileStatus[] substas = fs.listStatus(sta.getPath());
				for(FileStatus substa : substas) {
					show(substa);
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void showDetail(FileStatus sta) {
		System.out.println(sta.getPath()+" "+sta.getLen()+" "+sta.getOwner()+" "+sta.getAccessTime());
	}

}
