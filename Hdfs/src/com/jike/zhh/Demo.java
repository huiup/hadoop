package com.jike.zhh;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;




public class Demo {

	public void Download() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.157.27:9000"), new Configuration());
		
		FSDataInputStream in = fs.open(new Path("/student"));
			
		OutputStream out = new FileOutputStream("1.txt");
		
		byte[] data = new byte[1024];
		
		int i = -1;
		
		while((i=in.read(data))!=-1){
			out.write(data);
		}
		out.close();
		in.close();
		fs.close();
}
	@Test
	public void upload() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.157.27:9000"), new Configuration());
		
		FSDataOutputStream out = fs.create(new Path("/2.txt"));
		
		InputStream in = new FileInputStream("1.txt");
		
		IOUtils.copyBytes(in, out, 1024);
		out.close();
		in.close();
		fs.close();
	}
	@Test
	public void delete() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.157.27:9000"), new Configuration());

		fs.delete(new Path("/2.txt"),true);
		fs.close();
		
	}
	@Test
	public void mkdir() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.157.27:9000"), new Configuration());

		fs.mkdirs(new Path("/aaa/bbb/ccc/ddd"));
		fs.close();
	}
}