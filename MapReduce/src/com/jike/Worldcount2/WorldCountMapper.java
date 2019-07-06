package com.jike.Worldcount2;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorldCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		char[] arr = line.toCharArray();
//		String[] arr = line.split("");

		for(char word : arr){

			context.write(new Text(word+""), new LongWritable(1));
		}
	}
}
