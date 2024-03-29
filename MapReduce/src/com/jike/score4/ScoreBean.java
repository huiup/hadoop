package com.jike.score4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ScoreBean implements Writable{

	private String name;
//	private int month;
	private int chinese;
	private int english;
	private int math;
	
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
//	public int getMonth() {
//		return month;
//	}
//	public void setMonth(int month) {
//		this.month = month;
//	}
	public int getChinese() {
		return chinese;
	}
	public void setChinese(int chinese) {
		this.chinese = chinese;
	}
	public int getEnglish() {
		return english;
	}
	public void setEnglish(int english) {
		this.english = english;
	}
	public int getMath() {
		return math;
	}
	public void setMath(int math) {
		this.math = math;
	}
	

@Override
	public String toString() {
		return "ScoreBean [name=" + name + ", chinese=" + chinese + ", english=" + english + ", math=" + math + "]";
	}
//	@Override
//	public String toString() {
//		return "ScoreBean [name=" + name + ", month=" + month + ", chinese=" + chinese + ", english=" + english
//				+ ", math=" + math + "]";
//	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name=in.readUTF();
//		this.month=in.readInt();
		this.chinese=in.readInt();
		this.english=in.readInt();
		this.math=in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
//		out.writeInt(month);
		out.writeInt(chinese);
		out.writeInt(english);
		out.writeInt(math);
	}
	
}
