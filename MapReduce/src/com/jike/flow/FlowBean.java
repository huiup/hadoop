package com.jike.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private String phone;
	private String addr;
	private String name;
	private long flow;
	public FlowBean(){}
	public FlowBean(String phone,String addr,String name,long flow){
		this.phone = phone;
		this.addr = addr;
		this.name = name;
		this.flow = flow;
		
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getFlow() {
		return flow;
	}
	public void setFlow(long flow) {
		this.flow = flow;
	}
	@Override
	public String toString() {
		return "FlowBean [phone=" + phone + ", addr=" + addr + ", name=" + name + ", flow=" + flow + "]";
	}
	//反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.phone=in.readUTF();
		this.addr=in.readUTF();
		this.name=in.readUTF();
		this.flow=in.readLong();
	}
	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phone);
		out.writeUTF(addr);
		out.writeUTF(name);
		out.writeLong(flow);
		
		
	}
	
}
