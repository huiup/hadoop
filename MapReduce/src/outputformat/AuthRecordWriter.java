package outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AuthRecordWriter<K,V> extends RecordWriter<K, V>{

	
	private FSDataOutputStream out;
	public AuthRecordWriter(FSDataOutputStream out) {

		this.out=out;
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {

		out.write(key.toString().getBytes());
		//指定kv分隔符
		out.write("|".getBytes());
		
		out.write(value.toString().getBytes());
		//指定kv对之间的分隔符
		out.write("\b\n".getBytes());
	}

}
