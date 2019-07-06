package com.jike.FileSplit;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SplitMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {

		FileSplit fs = (FileSplit) context.getInputSplit();
		
		Path path = fs.getPath();
		String fileName=path.getName();
		String line=value.toString();
		System.out.println("行数据:"+line+"路径:"+path+"文件名:"+fileName);
		context.write(NullWritable.get(), NullWritable.get());
	}
}
