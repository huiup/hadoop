package com.jike.scoreInput;


import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class ScoreRecordReader extends RecordReader<Text,Text>{
	
	private FileSplit fs;
	private Text key;
	private Text value;
	private LineReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		fs=(FileSplit) split;
		Path path=fs.getPath();
		Configuration conf=context.getConfiguration();
		FileSystem system=path.getFileSystem(conf);
		InputStream in=system.open(path);
		reader=new LineReader(in);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key=new Text();
		value=new Text();
		Text tmp=new Text();
		int length=reader.readLine(tmp);
		if(length==0){
			return false;
		}else{
			//--tom
			//--math 80 
			//--english 90 
			//--设置输入key 人名
			key.set(tmp);
			
			for(int i=0;i<2;i++){
				reader.readLine(tmp);
				value.append(tmp.getBytes(),0, tmp.getLength());
				//--拼空格
				value.append(" ".getBytes(),0, 1);
			}
			return true;
		}

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		//--去掉首尾的空格
		return new Text(value.toString().trim());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		reader=null;
		
	}

}
