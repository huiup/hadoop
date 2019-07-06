package com.jike.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineNumberMapper extends Mapper<IntWritable, Text, IntWritable, Text>{

	@Override
	protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {


		//验证key，value
		context.write(key, value);
	}
}
