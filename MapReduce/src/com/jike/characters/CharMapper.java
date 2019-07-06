package com.jike.characters;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//<偏移量，文本内容，单词，次数>
public class CharMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//1.获取行数据	
		String line = value.toString();
		//2.按照空格切分单词
		char[] arr = line.toCharArray();
		//3.循环遍历数组，输出单词和数量
		for(char word : arr){
//			if(word.equals(" ")){
//				continue;
//			}
			context.write(new Text(word+""), new LongWritable(1));
		}
	}
}

