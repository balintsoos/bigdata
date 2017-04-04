package hu.elte.bd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BDMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final Text wordKey = new Text("");
	private final IntWritable wordNum = new IntWritable(1);

	public void map(LongWritable ikey, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		String[] words = fields[17].split(" ");
		for (String s : words) {
			wordKey.set(s);
			context.write(wordKey, wordNum);
		}

	}

}


