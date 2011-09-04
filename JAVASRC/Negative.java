//Negative.java - MapReduce program, emits:
//( WORD	USER,	COUNT)
//( TOTAL	USER,	COUNT)
//I consider negative words to be: "stupid, dumb, terrible, sucks, suck, awful, worst, crap, crappy, 
//Sean Hogan, 2011
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Scanner;

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


public class Negative {

	static String[] badWords = {"stupid","dumb","terrible","sucks","suck","awful","worst","crap","bastard","hate"};
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Scanner scnr = new Scanner(value.toString());
			scnr.useDelimiter("\t");
			String username = "";
			String message = "";
			  int pos = 0; 
		        while (scnr.hasNext()) {
		        	if (pos == 3) {
		        		username = scnr.next();
		        		pos++;
		        	}
		        	else if (pos == 7) {
		        		message = scnr.next().toLowerCase();
		        		Scanner sigspltr = new Scanner(message);
		        		sigspltr.useDelimiter("---"); //Lets use skip the signatures which are delimited by ---. Yes this introduce some error, but very insignificant
		        		try {
		        			message = sigspltr.next();
		        		}
		        		catch(NoSuchElementException e){
		        			String foo = "";
		        		}
		        		pos++;
		        	}
		        	else {
		        		scnr.next();
		        		pos++;
		        	}
		        }
		     Scanner msgScnr = new Scanner(message);
		     String maybeBad = "";
		     while (msgScnr.hasNext()) {
		    	 maybeBad = msgScnr.next();
		    	 for(String badWord : badWords) {
		    		 if (maybeBad.equals(badWord)) {
					//If you desire, uncomment these to get total word usage for a user,
					//or specific word usage for a user.
		    			// context.write(new Text("TOTAL\t"+username),new IntWritable(1));
		    			// context.write(new Text("WORD "+username+"\t"+maybeBad),new IntWritable(1));
		    			 context.write(new Text("SINGLE\t"+maybeBad),new IntWritable(1));
		    		 }
		    	 }
		     }
		    
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "Negative word count");
	    
	    job.setJarByClass(Negative.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("Negative"+args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
