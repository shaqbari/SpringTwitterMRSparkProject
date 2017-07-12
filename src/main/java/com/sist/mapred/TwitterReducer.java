package com.sist.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwitterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable result=new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum=0;
		for (IntWritable i : values) {
			//sum=sum+i.get();
			sum+=i.get();
		}
		result.set(sum);
		String data="\""+key.toString()+"\"";
		key.set(data);//단어를  ""로 묶는다. "aaa aaa aaa"
		context.write(key, result);
		
		
	}
	
	
	
	
	
}
