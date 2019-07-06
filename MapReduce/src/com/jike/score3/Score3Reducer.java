package com.jike.score3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Score3Reducer extends Reducer<Text, ScoreBean, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<ScoreBean> value, Reducer<Text, ScoreBean, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		long li = 0;
		Iterator<ScoreBean> it = value.iterator();
		while(it.hasNext()){
			ScoreBean next = it.next();
			li = next.getScore1()+next.getScore2()+next.getScore3();
		}
		context.write(key,new LongWritable(li));
	}
}