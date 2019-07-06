package com.jike.characters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		long count = 0;
		Iterator<LongWritable> it = value.iterator();
		// 循环遍历做叠加
		while (it.hasNext()) {
			count = count + it.next().get();

		}
		context.write(key, new LongWritable(count));
	}

}
