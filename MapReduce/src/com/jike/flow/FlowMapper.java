package com.jike.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split(" ");
		FlowBean fb = new FlowBean(arr[0],arr[1],arr[2],Long.parseLong(arr[3]));
		String phone = arr[0];
		context.write(new Text(phone), fb);
	}
}
