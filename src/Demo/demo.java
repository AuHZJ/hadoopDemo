package Demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class demo extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new demo(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(demo.class);
		return 0;
	}

}
