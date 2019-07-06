package com.jike.Invert;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertReducer2 extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		for(Text values:value){
			context.write(key,new Text(values.toString()));

		}
	}
}
