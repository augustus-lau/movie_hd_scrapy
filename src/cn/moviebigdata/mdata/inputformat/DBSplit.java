package cn.moviebigdata.mdata.inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class DBSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit{
	
	private int start;
	
	private int limit;
	
	
	
	public DBSplit() {
	}

	public DBSplit(int start, int limit) {
		super();
		this.start = start;
		this.limit = limit;
	}

	/**
	 * read yu write���� ��Ҫ��Ϊ�����������split�е��ֶ�����ֵ��
	 * ��Ϊsplist���ڲ�ͬ�ĵ���֮�����л�����
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.start = in.readInt();
		this.limit = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.start);
		out.writeInt(this.limit);
		
	}

	@Override
	public long getLength() throws IOException {
		
		return this.limit;
	}

	@Override
	public String[] getLocations() throws IOException {
		
		return new String[]{};
	}

	@Override
	public String toString() {
		return this.start + ":" + this.limit ;
	}

	
	
	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	
	
}
