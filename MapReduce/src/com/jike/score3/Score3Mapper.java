package com.jike.score3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Score3Mapper extends Mapper<LongWritable,Text,Text,ScoreBean>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ScoreBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(" ");
		ScoreBean sb = new ScoreBean(arr[0],Long.parseLong(arr[1]),Long.parseLong(arr[2]),Long.parseLong(arr[3]));
		String name = arr[0];
		context.write(new Text(name), sb);
	}
}
