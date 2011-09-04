/** TopicCount.java
 *	This takes in topicdata
 * 	and then sends out data for a histogram on how many
 * 	topics of a certain size were made,
 * 	who postsed the most topics,
 * 	what games had the most topics posted,
 * 	and a word count of topic titles.
 * 		
 * 	Sean Hogan, 2011
 */


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopicData {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        Text text = new Text();
	        String user = "";
	        IntWritable one = new IntWritable(1);
	        Scanner scnr = new Scanner(line);
	        scnr.useDelimiter("\t"); //Input lines are tab-delimited
	        int pos = 0; //Our position looking through a line, which has format
	        String title = "";
	        while (scnr.hasNext()) {
	        	if (pos == 2) {
	            	user = scnr.next();
	            }
	        	else if(pos == 1) {
	        		String game = scnr.next();
	        		text.set("GAME\t"+game);
	        		context.write(text,one);
	        	}
	        	else if(pos == 3) {
	            	title = scnr.next().toLowerCase();
	            }
	        	else if(pos == 4) {
	        		text.set("POSTS\t"+scnr.next());
	        		context.write(text,one);
	        	}
	        	else {
	        		scnr.next();
	        	}
	            pos++;
	        }
	        text.set("TOPICSBY\t"+user);
	        context.write(text,one);
	        title = title.toLowerCase();
	        scnr.close();
	        Scanner msgscanner = new Scanner(title);
	        while (msgscanner.hasNext()) {
	        	text.set("FREQ\t"+msgscanner.next());
	        	context.write(text,one);
	        }
	        
	        
		}
	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += 1;
			}
			context.write(key,new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
	if (args.length != 3) {
		System.out.println("Usage: input output #reducers");
		System.exit(1);
		
	}
        Job job = new Job(conf, "TopicData:WC,PostC,no.tops.by");
    
    job.setJarByClass(TopicData.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("TopicData"+args[1]));
        
    job.waitForCompletion(true);
	}
}
