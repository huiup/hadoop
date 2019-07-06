package com.jike.ip;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//<偏移量，文本内容，单词，次数>
public class IpMapper extends Mapper<LongWritable,Text,Text,NullWritable>{


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		context.write(new Text(value), NullWritable.get());
	}
		
	
}

