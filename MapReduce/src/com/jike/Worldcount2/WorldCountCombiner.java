package com.jike.Worldcount2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WorldCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
	
		long count = 0;
		for(LongWritable values:value){
			count=count+values.get();
		}
		context.write(key, new LongWritable(count));
	}
}
