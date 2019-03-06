package io;



import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IntegriyReadTest extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new IntegriyReadTest(), args);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Path rpath = new Path(conf.get("rpath"));
		Path lpath = new Path(conf.get("lpath"));
		RawLocalFileSystem rlfs = new RawLocalFileSystem();
		rlfs.initialize(URI.create(conf.get("rpath")), conf);
		FSDataInputStream rin = rlfs.open(rpath);
		BufferedReader rr = new BufferedReader(new InputStreamReader(rin));
		String rline = rr.readLine();
		System.out.println(rline);
		rr.close();
		System.out.println("---------------");
		//-------------------
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		FSDataInputStream lin = lfs.open(lpath);
		BufferedReader lr = new BufferedReader(new InputStreamReader(lin));
		String lline = lr.readLine();
		System.out.println(lline);
		rr.close();
		return 0;
	}

}
