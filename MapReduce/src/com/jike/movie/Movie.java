package com.jike.movie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Movie implements WritableComparable<Movie>{

	private String name;
	private int hot;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name=in.readUTF();
		this.hot=in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeInt(hot);
	}
	@Override
	public int compareTo(Movie o) {
		// TODO Auto-generated method stub
		//降序,把较大的数据放前面，由底层的sort源码来决定的
		return o.hot-this.hot;
		//升序，把小数放前面
		
//		return this.hot-o.hot;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getHot() {
		return hot;
	}
	public void setHot(int hot) {
		this.hot = hot;
	}
	@Override
	public String toString() {
		return "Movie [name=" + name + ", hot=" + hot + "]";
	}
	
}
