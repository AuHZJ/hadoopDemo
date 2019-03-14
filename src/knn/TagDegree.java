package knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 对于相似度排序时进行使用的 tag degree group三个属性 前两个用来存放数据，group属性控制分组 故group给出相同值即可
 * 
 * @author Au
 *
 */
public class TagDegree implements WritableComparable<TagDegree> {
	// 文件前缀名
	private Text tag = new Text();
	// 相似度
	DoubleWritable degree = new DoubleWritable();
	private Text group = new Text("1");

	public TagDegree() {
	}

	public TagDegree(String tag, double degree) {
		this.tag = new Text(tag);
		this.degree = new DoubleWritable(degree);
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
	}

	public DoubleWritable getDegree() {
		return degree;
	}

	public void setDegree(DoubleWritable degree) {
		this.degree = new DoubleWritable(degree.get());
	}

	public Text getGroup() {
		return group;
	}

	public void setGroup(Text group) {
		this.group = new Text(group.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		degree.readFields(in);
		group.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		degree.write(out);
		group.write(out);
	}

	@Override
	public int compareTo(TagDegree o) {
		return o.degree.compareTo(this.degree);
	}

	@Override
	public String toString() {
		return this.tag.toString() + "\t" + this.degree.toString();
	}
}
