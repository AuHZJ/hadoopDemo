package io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;

public class WritebleTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Integer i = new Integer(5);
		// 5通过java序列化成二进制数据占多少字节
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(out);
		//oos.write(i);
		oos.writeObject(i);
		oos.flush();
		byte[] ba = out.toByteArray();
		System.out.println("java字节数：" + ba.length);
		oos.close();
		// 5通过hadoop序列化成二进制数据占多少字节
		IntWritable iw = new IntWritable(5);
		ByteArrayOutputStream out2 = new ByteArrayOutputStream();
		ObjectOutputStream oos2 = new ObjectOutputStream(out2);
		iw.write(oos2);
		byte[] ba2 = out2.toByteArray();
		System.out.println("hadoop字节数：" + ba2.length);
		oos2.close();
		
	}

}
