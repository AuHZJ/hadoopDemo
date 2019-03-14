package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



//打成jar包运行：hadoop jar test.jar hadoop/ShowFileContent

public class ShowFileContent {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// 1.
		Configuration conf = new Configuration();
		System.out.println(conf);
		conf.set("fs.defaultFS", "hdfs://192.168.1.103:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			System.out.println(fs);
			FSDataInputStream in = fs.open(new Path("/a.txt"));
			
			IOUtils.copyBytes(in, System.out, conf);
			
//			String line = null;
//			while((line = in.readLine()) != null) {
//				System.out.println(line);
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
