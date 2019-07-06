package com.jike.score;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

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
//long count = 0;
//Iterator<LongWritable> it = value.iterator();
//while(it.hasNext()){
//	count = count + it.next().get();
//	
//}
//context.write(key, new LongWritable(count));