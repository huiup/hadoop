package com.jike.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


//定义：
//输入key为每行行号
//输入value为每行内容

public class LineNumberFormat extends FileInputFormat<IntWritable, Text>{

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {

		
		return new LineNumberReader();
	}

	
}
