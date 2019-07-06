package com.jike.score2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Score2Reducer extends Reducer<Text,LongWritable,Text,LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		for(LongWritable score : value){
			sum += score.get() ;
		}
		context.write(key, new LongWritable(sum));

	}
}