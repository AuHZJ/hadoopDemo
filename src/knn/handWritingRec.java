package knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import knn.GetLastResult.GLRMapper;
import knn.GetLastResult.GLRReducer;
import knn.GetSimilarityDegree.GSDMapper;
import knn.GetTopK.DMapper;
import knn.GetTopK.DReducer;
import knn.SortByDegree.SBDMapper;

public class handWritingRec extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new handWritingRec(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		// *清理上次运行的目录文件
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/knn_data/unkown"), true);
		fs.delete(new Path("/knn_data/train"), true);
		fs.delete(new Path("/knn_data/gsd_result"), true);
		fs.delete(new Path("/knn_data/gsd_result_sorted"), true);
		fs.delete(new Path("/knn_data/gsd_result_sorted_topk"), true);
		fs.delete(new Path("/knn_data/gsd_result_sorted_topk_lastResult"), true);
		// 1 待识别图片 二值化并上传到hdfs
		Path inpath = new Path("unkown/unkown.png");
		Path outpath = new Path("/knn_data/unkown");
		UnkownPicToBin.putToHdfs(inpath, outpath, conf);
		// 2 计算相似度
		Job gsd_job = Job.getInstance(conf, "GetSimilarityDegree");
		gsd_job.setJarByClass(GetSimilarityDegree.class);
		gsd_job.setMapperClass(GSDMapper.class);
		gsd_job.setMapOutputKeyClass(Text.class);
		gsd_job.setMapOutputValueClass(DoubleWritable.class);
		gsd_job.setInputFormatClass(SequenceFileInputFormat.class);
		gsd_job.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileInputFormat.addInputPath(gsd_job, new Path("/knn_data/train_bin"));
		TextOutputFormat.setOutputPath(gsd_job, new Path("/knn_data/gsd_result"));
		// 3 按照相似度进行排序
		Job sbd_job = Job.getInstance(conf, "SortByDegree");
		sbd_job.setJarByClass(SortByDegree.class);
		sbd_job.setMapperClass(SBDMapper.class);
		sbd_job.setMapOutputKeyClass(TagDegree.class);
		sbd_job.setMapOutputValueClass(NullWritable.class);
		sbd_job.setInputFormatClass(TextInputFormat.class);
		sbd_job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(sbd_job, new Path("/knn_data/gsd_result/part-r-00000"));
		TextOutputFormat.setOutputPath(sbd_job, new Path("/knn_data/gsd_result_sorted"));
		sbd_job.setGroupingComparatorClass(TagDegreeGroupComparator.class);
		// 4 knn统计平均相似度和标签个数
		Job gtk_job = Job.getInstance(conf, "GetTopK");
		gtk_job.setJarByClass(GetTopK.class);
		gtk_job.setMapperClass(DMapper.class);
		gtk_job.setMapOutputKeyClass(Text.class);
		gtk_job.setMapOutputValueClass(Text.class);
		gtk_job.setReducerClass(DReducer.class);
		gtk_job.setOutputKeyClass(Text.class);
		gtk_job.setOutputValueClass(Text.class);
		gtk_job.setInputFormatClass(TextInputFormat.class);
		gtk_job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(gtk_job, new Path("/knn_data/gsd_result_sorted/part-r-00000"));
		TextOutputFormat.setOutputPath(gtk_job, new Path("/knn_data/gsd_result_sorted_topk"));
		// 5 获得最终结果
		Job glr_job = Job.getInstance(conf, "GetLastResult");
		glr_job.setJarByClass(GetLastResult.class);
		glr_job.setMapperClass(GLRMapper.class);
		glr_job.setMapOutputKeyClass(Text.class);
		glr_job.setMapOutputValueClass(TagAvgNum.class);
		glr_job.setReducerClass(GLRReducer.class);
		glr_job.setOutputKeyClass(Text.class);
		glr_job.setOutputValueClass(NullWritable.class);
		glr_job.setInputFormatClass(TextInputFormat.class);
		glr_job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(glr_job, new Path("/knn_data/gsd_result_sorted_topk/part-r-00000"));
		TextOutputFormat.setOutputPath(glr_job, new Path("/knn_data/gsd_result_sorted_topk_lastResult"));
		// 6 装配工作流
		ControlledJob gsd_cj = new ControlledJob(gsd_job.getConfiguration());
		ControlledJob sbd_cj = new ControlledJob(sbd_job.getConfiguration());
		ControlledJob gtk_cj = new ControlledJob(gtk_job.getConfiguration());
		ControlledJob glr_cj = new ControlledJob(glr_job.getConfiguration());
		glr_cj.addDependingJob(gtk_cj);
		gtk_cj.addDependingJob(sbd_cj);
		sbd_cj.addDependingJob(gsd_cj);
		JobControl control = new JobControl("handWritingRec");
		control.addJob(gsd_cj);
		control.addJob(sbd_cj);
		control.addJob(gtk_cj);
		control.addJob(glr_cj);
		Thread t = new Thread(control);
		t.start();
		while (!control.allFinished()) {
		}
		System.out.println("图片识别完毕！");
		// 7 输出结果
		FSDataInputStream in = fs.open(new Path("/knn_data/gsd_result_sorted_topk_lastResult/part-r-00000"));
		System.out.print("结果为：");
		IOUtils.copyBytes(in, System.out, conf);
		in.close();
		fs.close();
		System.exit(0);
		return 0;
	}

}
