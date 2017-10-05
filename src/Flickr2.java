
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Flickr2 {

	static int k;

	public static class MyMapper1 extends Mapper<LongWritable, Text, StringAndString, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split_text = value.toString().split("\t");
			String tags = split_text[8];
			String[] tags_line = tags.split(",");

			double longitude = Double.parseDouble(split_text[10]);
			double latitude = Double.parseDouble(split_text[11]);
			Country pays = Country.getCountryAt(latitude, longitude);
			if (pays == null)
				return;
			System.out.println(pays.toString());
			for (int i = 0; i < tags_line.length; i++) {
				context.write(new StringAndString(pays.toString(), tags_line[i]), new IntWritable(1));
			}

		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}
	/*
	 * public static class MyCombiner1 extends Reducer<StringAndString,
	 * IntWritable, StringAndString, IntWritable> {
	 * 
	 * @Override protected void reduce(StringAndString key,
	 * Iterable<IntWritable> values, Context context) throws IOException,
	 * InterruptedException { HashMap<String, Integer> mapTag = new HashMap<>();
	 * 
	 * for (Iterator<Text> iter = values.iterator(); iter.hasNext();) { Object
	 * tag = iter.next(); Integer value_tag = mapTag.get(tag); if (value_tag ==
	 * null) { mapTag.put(tag.toString(), 1); } else {
	 * mapTag.put(tag.toString(), value_tag + 1); } }
	 * 
	 * for (Entry<String, Integer> pair : mapTag.entrySet()) {
	 * context.write(key, new StringAndInt(pair.getKey(), pair.getValue())); }
	 * 
	 * } }
	 */

	public static class MyReducer1 extends Reducer<StringAndString, IntWritable, StringAndString, IntWritable> {
		@Override
		protected void reduce(StringAndString key, Iterable<IntWritable> values, Context context)

				throws IOException, InterruptedException {
			int sum = 0;
			for (Iterator<IntWritable> iter = values.iterator(); iter.hasNext();) {
				IntWritable tag_occurence = iter.next();
				sum += tag_occurence.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class MyMapper2 extends Mapper<StringAndString, IntWritable, Text, StringAndInt> {
		@Override
		protected void map(StringAndString key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.getPays()), new StringAndInt(key.getTag().toString(), value.get()));
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class MyReducer2 extends Reducer<Text, StringAndInt, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)

				throws IOException, InterruptedException {
			MinMaxPriorityQueue<StringAndInt> myPriorityQueue = MinMaxPriorityQueue.create();

			for (Iterator<StringAndInt> iter = values.iterator(); iter.hasNext();) {
				StringAndInt tag_occurence = iter.next();
				myPriorityQueue.add(tag_occurence.clone());
			}

			for (int i = 0; i < k; i++) {
				StringAndInt loc = myPriorityQueue.pollLast();
				if (loc != null) {
					System.out.println(key+", "+loc.getTagName()+", "+loc.getTagOccurence());
					context.write(key, new Text(loc.getTagName()+", "+loc.getTagOccurence().get()));
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		System.out.println(input);
		String output = otherArgs[1];
		k = Integer.parseInt(otherArgs[2]);
		Path output_intermediate_file;

		Job job1 = Job.getInstance(conf, "Question0_0");
		job1.setNumReduceTasks(1);
		job1.setJarByClass(Flickr2.class);

		job1.setMapperClass(MyMapper1.class);
		job1.setMapOutputKeyClass(StringAndString.class);
		job1.setMapOutputValueClass(IntWritable.class);
		// job1.setCombinerClass(MyCombiner.class);

		job1.setReducerClass(MyReducer1.class);
		job1.setOutputKeyClass(StringAndString.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(TextInputFormat.class);

		output_intermediate_file = new Path("intermediate_file");
		FileOutputFormat.setOutputPath(job1, output_intermediate_file);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Question0_0");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(Flickr2.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(StringAndInt.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, output_intermediate_file);
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}