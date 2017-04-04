package hu.elte.bd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BDCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	private final IntWritable wordnum = new IntWritable(1);
	
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		wordnum.set(sum);
		context.write(_key, wordnum);
	}

}
