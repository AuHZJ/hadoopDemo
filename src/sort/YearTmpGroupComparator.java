package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearTmpGroupComparator extends WritableComparator {
	public YearTmpGroupComparator() {
		super(YearTmp.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		YearTmp yt1 = (YearTmp) a;
		YearTmp yt2 = (YearTmp) b;
		return yt1.getYear().compareTo(yt2.getYear());
	}
}
