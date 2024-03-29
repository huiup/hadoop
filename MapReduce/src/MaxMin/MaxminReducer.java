package MaxMin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxminReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long min = 1000;
		for(LongWritable res :value){
			if(res.get()<min){
				min=res.get();
			}
		}
		context.write(key, new LongWritable(min));

	}
}