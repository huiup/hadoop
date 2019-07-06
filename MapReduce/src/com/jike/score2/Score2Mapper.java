package com.jike.score2;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Score2Mapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String[] arr = line.split(" ");
		
		context.write(new Text(arr[0]), new LongWritable(Long.parseLong(arr[1])));
		
	}
}
