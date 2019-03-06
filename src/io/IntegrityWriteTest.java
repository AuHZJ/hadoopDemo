package io;

import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IntegrityWriteTest extends Configured implements Tool{
	// LocalFileSystem
	// RawLocalFileSystem

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new IntegrityWriteTest(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Path rpath = new Path(conf.get("rpath"));
		Path lpath = new Path(conf.get("lpath"));
		RawLocalFileSystem rlfs = new RawLocalFileSystem();
		rlfs.initialize(URI.create(conf.get("rpath")), conf);
		FSDataOutputStream rout = rlfs.create(rpath);
		PrintWriter rpw = new PrintWriter(rout);
		rpw.println("hhhhhhhhhh");
		rpw.flush();
		rpw.close();
		//---------------------
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		FSDataOutputStream lout = lfs.create(lpath);
		PrintWriter lpw = new PrintWriter(lout);
		lpw.println("hhhhhhhhhh");
		rpw.flush();
		rpw.close();
		return 0;
	}

}
