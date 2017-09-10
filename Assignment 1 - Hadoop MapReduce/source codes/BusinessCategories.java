package yelp.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BusinessCategories {

	public static class BusinessCategoriesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		static String total_record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_category = new Text();
			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split("::");
			if (fields.length == 3) {
				if ((fields[1].contains("Palo Alto"))) {
					String[] category_line = fields[2].toString().split("(List\\()|(\\))|(,)");
					for(String category:category_line){
						business_category.set(category.trim());
						NullWritable nullOb = NullWritable.get();
						context.write(business_category, nullOb);
					}	
				}
				total_record = "";
			}
		}

	}

	public static class BusinessReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		// private NullWritable result = new NullWritable();

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			// int sum = 0; // initialize the sum for each keyword
			NullWritable nullOb = NullWritable.get();
			context.write(key, nullOb);

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}
		Job job = new Job(conf, "Palo Alto Count");
		job.setJarByClass(BusinessCategories.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(BusinessCategoriesMapper.class);
		job.setCombinerClass(BusinessReduce.class);
		job.setReducerClass(BusinessReduce.class);
		// job.setNumReduceTasks(0);
		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);

	}
}
