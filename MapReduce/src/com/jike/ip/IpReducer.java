package com.jike.ip;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<NullWritable> value,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) 
					throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
	}

}
