/**The way this works is quite funky.
First, we need to partition our table of prefixes (e.g., 4-14, 55-1203, etc)
into however many mappers we want to use. In my case I am using around 130.
These files reside locally. Now you must make just as many files, that only contain these file names.
Each mapper takes this file name, appends it to some python script it runs.

So say I have this set of 1000 prefixes, it's called xbd. Then I make a file h-in-xbd which just contains "xbd"

So the mapper takes this one file, and runs python script.py /path/to/xbd, and foom, runs a script on all such data (stored in HDFS)
that has 500 posts in it, and saves that to local disk.

And ta-da!
**/


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




public class Get500s {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//Edit this to point to where you need it to
			String cmd = "/usr/bin/python /home/seanhogan/GAMEFAQS/find500s.py /home/seanhogan/GAMEFAQS/prefixes/"+value.toString();

			Runtime run = Runtime.getRuntime();
			Process pr = run.exec(cmd);
			pr.waitFor();
			context.write(new Text("er"),new IntWritable(1));
		
		    
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
		context.write(new Text("foo"),new IntWritable(1));
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "Get 500 topic's message data");
	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    job.setJarByClass(Get500s.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("Get500s"+args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
