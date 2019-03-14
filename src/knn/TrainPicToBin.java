package knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainPicToBin extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrainPicToBin(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path inpath = new Path("/knn_data/train");
		Path outpath = new Path("/knn_data/train_bin");
		allPicsToBin(inpath, outpath, conf);
		return 0;
	}

	public static void allPicsToBin(Path inpath, Path outpath, Configuration conf)
			throws FileNotFoundException, IOException {
		// inpath��hdfs�е�Ŀ¼
		FileSystem fs = FileSystem.get(conf);
		// ͨ���ļ�ϵͳ��ȡĳ��Ŀ¼�µ������ļ�
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(inpath, true);
		// ��ȡÿ���ļ����ݣ���ֵ���������sequencefile
		// ѡ��1 sequencefile���·��
		SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(outpath);
		// ѡ��2 sequencefile key���ͣ��ļ�ǰ׺��
		SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(Text.class);
		// ѡ��3 sequencefile value���ͣ���ֵ������
		SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(Text.class);
		// sequencefile�����
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, option1, option2, option3);
		// ��������key��value��ֵ
		Text k = new Text();
		Text v = new Text();
		while (files.hasNext()) {
			// ��ֵ��ÿ��ͼƬ
			LocatedFileStatus file = files.next();
			// ��ͼƬ��ǰ׺�����ó�k
			String name = file.getPath().getName();
			String new_name = name.substring(0, name.indexOf("."));
			k.set(new_name);
			// �Ѷ�ֵ��������ó�v
			FSDataInputStream in = fs.open(file.getPath());
			// ���ö�ֵ������
			String bin_line = picToBin(in);
			v.set(bin_line);
			// ׷�ӵ�sequencefile
			writer.append(k, v);
		}
		writer.close();
	}

	private static String picToBin(FSDataInputStream in) throws IOException {
		BufferedImage img = ImageIO.read(in);
		StringBuffer sb = new StringBuffer();
		int height = img.getHeight();
		int width = img.getWidth();
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int gray_rgb = gray.getRGB();
				if (rgb > gray_rgb) {
					sb.append("1");
					System.out.print("1");
				} else {
					sb.append("0");
					System.out.print("0");
				}
			}
			System.out.println();
		}
		return sb.toString();
	}
}
