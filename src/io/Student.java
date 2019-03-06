package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;



public class Student implements WritableComparable{
	
	private IntWritable id = new IntWritable();
	private Text name = new Text();
	private DoubleWritable weight = new DoubleWritable();
	
	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = new IntWritable(id.get());
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public DoubleWritable getWeight() {
		return weight;
	}

	public void setWeight(DoubleWritable weight) {
		this.weight = weight;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		getId().readFields(arg0);
		name.readFields(arg0);
		weight.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		id.write(arg0);
		name.write(arg0);
		weight.write(arg0);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Student s = (Student)o;
		int result = (int)(this.weight.get()*10 - s.weight.get()*10);
		return result;
	}

}
