package com.jike.score4;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, ScoreBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ScoreBean>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split(" ");
		String name = arr[1];
		int score = Integer.parseInt(arr[2]);
		FileSplit fs = (FileSplit) context.getInputSplit();
		 String fileName = fs.getPath().getName();

		ScoreBean sb = new ScoreBean();
		sb.setName(name);
		if(fileName.equals("chinese.txt")){
			sb.setChinese(score);
		}
		if(fileName.equals("english.txt")){
			sb.setEnglish(score);
		}
		if(fileName.equals("math.txt")){
			sb.setMath(score);
		}
////		ScoreBean sb = new ScoreBean(arr[0], Long.parseLong(arr[1]), Long.parseLong(arr[2]), Long.parseLong(arr[3]));
		context.write(new Text(sb.getName()), sb);
	}

}
