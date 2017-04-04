package hu.elte.bd;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BDReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();

	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		if (sum > 5) {
			countMap.put(_key, new IntWritable(sum));
		}
	}
	
	protected void cleanup(Context context) {
		Map<Text, IntWritable> sortedMap = sortedByValues(20);
		
		for (Text key : sortedMap.keySet()) {
			try {
				context.write(key, sortedMap.get(key));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private Map<Text, IntWritable> sortedByValues(int i) {
		List<Map.Entry<Text, IntWritable>> list = new LinkedList<>(countMap.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<Text, IntWritable>>() {
			public int compare(Map.Entry<Text, IntWritable> a, Map.Entry<Text, IntWritable> b) {
				return (a.getValue().compareTo(b.getValue()));
			}
		});
		
		int counter = 0;
		
		Map<Text, IntWritable> result = new LinkedHashMap<>();
		
		for (Map.Entry<Text, IntWritable> entry : list) {
			if (counter == 20) {
				break;
			}
			
			counter++;
			result.put(entry.getKey(), entry.getValue());
		}
		
		return result;
	}

}
