package com.jike.Totalsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalsortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
					throws IOException, InterruptedException {

		String line = value.toString();
		String[] arr = line.split(" ");
		for(String num: arr){
			context.write(new IntWritable(Integer.parseInt(num)), new IntWritable(1));
		}
	}
}