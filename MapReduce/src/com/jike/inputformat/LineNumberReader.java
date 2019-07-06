package com.jike.inputformat;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class LineNumberReader extends RecordReader<IntWritable, Text>{

	//输入key
	private IntWritable key;
	//输入value
	private Text value;
	
	//文件切片对象
	private FileSplit fs;
	//由Hadoop提供的工具包，可以按行读取文件数据
	private LineReader lineReader;
	
	//初始行号
	private int lineNumber=0;
	
	/*
	 * 组件初始化
	 * 一般用于资源初始化(初始化切片对象，行读取器)
	 * */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 初始化切片对象
		fs=(FileSplit)split;
		//获取处理文件的路径
		Path path = fs.getPath();
		//获取环境参数对象
		Configuration conf = context.getConfiguration();
		//获取HDFS对象
		FileSystem system = path.getFileSystem(conf);
		//获取文件的输入流
		InputStream in=system.open(path);
		//初始化行读取器，使其有文件数据可读
		lineReader = new LineReader(in);
	}
	/*
	 * 此方法会迭代多次，取决于返回的boolean类型
	 * 如果返回ture，则会被再次调用
	 * 如果返回false，则被终止
	 * 
	 * 此方法的作用：就是不断读取文件的行数据，直到没有行数据可读
	 * 则终止
	 * */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//每次调用方法时，都重新的初始化输入key
		key=new IntWritable();
		//每次调用方法时，都重新的初始化输入value
		value=new Text();
		Text temp=new Text();
		//readLine方法用于读取行数据
		//此方法每调用一次，就会读取一行数据，并把数据存到指定的变量里
		//返回值代表读取此行数据的字节数
		int length = lineReader.readLine(temp);
		
		if(length==0){//表示没有行数据可读
			return false;
		}else{
			lineNumber++;
			//行号赋值
			key.set(lineNumber);
			
			//
			//行数据赋值
			lineReader.readLine(value);
			return true;
		}
		
	}
	
	

	/*
	 * 将输入key传给Mapper
	 * 
	 * */
	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	/*
	 * 将输入value传给Mapper
	 * 
	 * */
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	/*
	 * 资源清理工作
	 * */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineReader=null;
	}
	

	

}
