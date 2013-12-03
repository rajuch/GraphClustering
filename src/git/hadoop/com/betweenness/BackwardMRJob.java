package com.betweenness;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class BackwardMRJob extends BaseJob {

	public static class BackwardMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node inNode = new Node(value.toString());
			String path = inNode.getPath();
			String[] tokens = path.split(",");
			for (int i = 1; i < tokens.length; i++) {
				if (!tokens[i - 1].equalsIgnoreCase("source")) {
					context.write(new Text(tokens[i - 1] + "-" + tokens[i]),
							one);
				}
			}

		}
	}

	public static class BackwardReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

	// method to set the configuration for the job and the mapper and the
	// reducer classes
	private Job getJobConf(String[] args) throws Exception {

		JobInfo jobInfo = new JobInfo() {
			@Override
			public Class<? extends Reducer> getCombinerClass() {
				return null;
			}

			@Override
			public Class<?> getJarByClass() {
				return BackwardMRJob.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return BackwardMapper.class;
			}

			@Override
			public Class<?> getOutputKeyClass() {
				return Text.class;
			}

			@Override
			public Class<?> getOutputValueClass() {
				return IntWritable.class;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				return BackwardReducer.class;
			}
		};

		return setupJob("BackwardMRJob", jobInfo);

	}

	public int run(String[] args) throws Exception {

		Job job = getJobConf(args);
		String input, output;
		input = args[0];
		output = args[1];

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: <in> <output name> <backwardMR output name>");
		}
		int res = ToolRunner.run(new Configuration(), new BackwardMRJob(), args);
		System.exit(res);
	}
}
