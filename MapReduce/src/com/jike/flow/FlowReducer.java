package com.jike.flow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> value, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		FlowBean fb = new FlowBean();
		Iterator<FlowBean> it = value.iterator();
		while(it.hasNext()){
			FlowBean next = it.next();
			fb.setPhone(next.getPhone());
			fb.setAddr(next.getAddr());
			fb.setName(next.getName());
			fb.setFlow(fb.getFlow()+next.getFlow());
		}
		context.write(key, fb);
	}
}
