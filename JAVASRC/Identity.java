//Identity mapper/reducer. used to combine ridiculous amounts of small files into sizeable CHUNX
//Sean Hogan, 2011

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



	public class Identity {
		public static class Map extends Mapper<LongWritable, Text, Text, Text> {
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		       context.write(value,new Text(""));
			}
		}
		
		
		public static class Reduce extends Reducer<Text, Text, Text, Text> {
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				for (Text value : values) { //Identity...
					context.write(key,value);
				}
		}
		}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	        
		Job job = new Job(conf, "Identity");
		if (args.length != 3) {
			System.out.println("Usage: hadoop jar Identity.jar Identity <input> <output> <# Reducers>");
		    	System.exit(1);
		}
	    job.setJarByClass(Identity.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    

	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	}
}

