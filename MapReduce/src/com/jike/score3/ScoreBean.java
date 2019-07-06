package com.jike.score3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ScoreBean implements Writable{

	private String name;
	private long score1;
	private long score2;
	private long score3;
	
	public ScoreBean(){}
	public ScoreBean(String name,long score1,long score2,long score3){
		this.name=name;
		this.score1=score1;
		this.score2=score2;
		this.score3=score3;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getScore1() {
		return score1;
	}
	public void setScore1(long score1) {
		this.score1 = score1;
	}
	public long getScore2() {
		return score2;
	}
	public void setScore2(long score2) {
		this.score2 = score2;
	}
	public long getScore3() {
		return score3;
	}
	public void setScore3(long score3) {
		this.score3 = score3;
	}
	@Override
	public String toString() {
		return "ScoreBean [name=" + name + ", score1=" + score1 + ", score2=" + score2 + ", score3=" + score3 + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name=in.readUTF();
		this.score1=in.readLong();
		this.score2=in.readLong();
		this.score3=in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeLong(score1);
		out.writeLong(score2);
		out.writeLong(score3);
	}
	
}
