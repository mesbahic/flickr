
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Flickr {

	static int k;

	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
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
				System.out.println(pays.name() + ", " + tags_line[i]);
				context.write(new Text(pays.name()), new StringAndInt(tags_line[i], 1));
			}

		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> mapTag = new HashMap<>();

			for (Iterator<StringAndInt> iter = values.iterator(); iter.hasNext();) {
				StringAndInt tag = iter.next();
				Integer value_tag = mapTag.get(tag.getTagName().toString());
				if (value_tag == null) {
					mapTag.put(tag.toString(), 1);
				} else {
					mapTag.put(tag.toString(), value_tag + 1);
				}
			}

			for (Entry<String, Integer> pair : mapTag.entrySet()) {
				context.write(key, new StringAndInt(pair.getKey(), pair.getValue()));
			}

		}
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)

				throws IOException, InterruptedException {
			MinMaxPriorityQueue<StringAndInt> myPriorityQueue = MinMaxPriorityQueue.create();

			for (Iterator<StringAndInt> iter = values.iterator(); iter.hasNext();) {
				StringAndInt loc = iter.next();
				System.out.println(key + ", " + loc.getTagName().toString() + ", " + loc.getTagOccurence().get());
				myPriorityQueue.add(loc.clone());
			}
			// System.out.println(mapTag);

			for (int i = 0; i < k; i++) {
				StringAndInt loc = myPriorityQueue.pollLast();
				if (loc != null) {
					System.out.println(key + ", " + loc.getTagName() + ", " + loc.getTagOccurence());
					context.write(key, new Text(loc.getTagName() + ", " + loc.getTagOccurence().get()));
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
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		k = Integer.parseInt(otherArgs[2]);

		// Over write file before starting
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		Job job = Job.getInstance(conf, "Question0_0");
		job.setNumReduceTasks(3);
		job.setJarByClass(Flickr.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);
		job.setCombinerClass(MyCombiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}