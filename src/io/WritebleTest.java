package io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;

public class WritebleTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Integer i = new Integer(5);
		// 5ͨ��java���л��ɶ���������ռ�����ֽ�
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(out);
		//oos.write(i);
		oos.writeObject(i);
		oos.flush();
		byte[] ba = out.toByteArray();
		System.out.println("java�ֽ�����" + ba.length);
		oos.close();
		// 5ͨ��hadoop���л��ɶ���������ռ�����ֽ�
		IntWritable iw = new IntWritable(5);
		ByteArrayOutputStream out2 = new ByteArrayOutputStream();
		ObjectOutputStream oos2 = new ObjectOutputStream(out2);
		iw.write(oos2);
		byte[] ba2 = out2.toByteArray();
		System.out.println("hadoop�ֽ�����" + ba2.length);
		oos2.close();
		
	}

}
