package hu.elte.bd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BDDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(hu.elte.bd.BDDriver.class);
		job.setMapperClass(hu.elte.bd.BDMapper.class);
		job.setCombinerClass(hu.elte.bd.BDCombiner.class);

		job.setReducerClass(hu.elte.bd.BDReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/user/hadoop/tweets"));
		FileOutputFormat.setOutputPath(job, new Path("feladat2-2out"));

		if (!job.waitForCompletion(true))
			return;
	}

}
