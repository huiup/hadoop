package MaxMin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxminMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(8,12);
		long temp = Long.parseLong(line.substring(18));
		context.write(new Text(year), new LongWritable(temp));
	}
}
