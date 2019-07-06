package com.jike.scoreInput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreDefaultMapper extends Mapper<LongWritable,Text,Text,Text>{
	/*
	 * jim	math 60 english 60
	   rose	math 75 english 100
       tom	math 80 english 90
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String name=line.split("\t")[0];
		String score=line.split("\t")[1];
		context.write(new Text(name), new Text(score));
	}
}
