//MessageWordUserCount : A MapReduce program that takes tab-delimited lines of the form
//Game system	Board Name	Topic Name	Poster Name	Message# PostDate PostTime	Message Contents
//And outputs:
//[TOTAL Word	Frequency-of-word]
//[POSTCOUNT User #Posts-by-user]
//[TIME timeintoweek freq] -E.g., "TIME 4 x" means x posts were made from 4 to 5 AM pacific time on a Sunday over all posts
//[LENGTH len freq] -Length of a message
//[GAME gamename freq] -For finding how often a post occurs in a game
//[ALLDATE date freq] -For finding dates with the most posts
//[DATE game date freq]  -For finding dates with the most posts for a game
//Sean Hogan, 2011

import java.io.IOException;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MsgData {
    static String[] junk = {"<br","/>","<b>","</b>","<i>","</i>","<p>","</p>"};
	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	         //All the stuff on the line, IN LOWERCASE!!!
	    	String line = value.toString();
	    	IntWritable one = new IntWritable(1);
	        Text text = new Text();
	        String username = "";
	        String message = "";
	        String date = "";
	        String game = "";
	        int dLen = 0; //Will be raw length of message - no punctuation length of message
	        Scanner scnr = new Scanner(line);
	        scnr.useDelimiter("\t"); //Input lines are tab-delimited
	        int pos = 0; //Our position looking through a line, which has format
	        while (scnr.hasNext()) {
	        	if (pos == 1) {
	        		game = scnr.next();
	        		text.set("GAME\t"+game); //Count posts in some game
	        		context.write(text,one);
	        	}
	        	else if (pos == 3) {
	        		username = scnr.next();
	        	}
	        	else if (pos == 5) {
	        		date = scnr.next();
	        		text.set("DATE\t"+game+"\t"+date);
	        		context.write(text,one);
	        		text.set("ALLDATE\t"+date);
	        		context.write(text,one);
	        	}
	        	else if (pos == 6) {
	        		String time = scnr.next();
	        		String datetime = date+" "+time;
	        		long msFromEpoch = Date.parse(datetime);
	        		Date dateOb = new Date();
	        		dateOb.setTime(msFromEpoch);
	        		
	        		int day = dateOb.getDay();
	        		int hours = dateOb.getHours();
	        		
	        		String timeintoweek = Integer.toString(24*day + hours);
	        		text.set("TIME\t"+timeintoweek);
	        		context.write(text,one);
	        		
	        		
	        		
	        	}
	        	else if (pos == 7) {
	        		Scanner sigspltr = new Scanner(message);
				//Lets us skip the signatures which are delimited by ---. 
				//Yes this introduce some error if someone willingly uses "---" in their message, but that is rare
	        		sigspltr.useDelimiter("---"); 
	        		try {
	        			message = sigspltr.next().toLowerCase();
	        		}
	        		catch(NoSuchElementException e){
	        			message = scnr.next().toLowerCase();
	        		}	        						
	        		int origLen = message.length();			
	        		context.write(text,one);
				//For the word count we'd like to ignore punctuation.
	        		message = message.replaceAll("[!-?,;()*]"," "); 
	        		message = message.replaceAll("[.]","");
	        		int noPuncLen = message.length();
				//However we'd like to make sure the punctuation is included in the message length.
	        		dLen = origLen - noPuncLen; 
	        	}
	        	else scnr.next();
	        	pos++;
	        }
	        text.set("POSTCOUNT\t"+username); //Count posts by a user
        	context.write(text,one);
        	
	        Scanner msgScnr = new Scanner(message);
	        
	        int trueLen = dLen;
	        while (msgScnr.hasNext()) {
	        	String word = msgScnr.next();
	        	Boolean isJunk = false; 
			//Filter out HTML tags
	        	for (String junkword : junk) {
	        		if (word.equals(junkword)) {
	        			isJunk = true;
	        		}
	        	}
	        	if(isJunk == false) {
	        		trueLen += word.length() + 1; //Adding the one to count for spaces between words
	        		text.set("TOTAL\t"+word);
	        		context.write(text,one); 
	        	}
	      
	        }
	        
	        String length = Integer.toString(trueLen);
	        text.set("LENGTH\t"+length); 
	        context.write(text,one);
	        
	    	
	    }
	 } 
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		//Nothing new here, folks. Typical reduce for a word count.
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
		 
		if (args.length != 3) {
			System.out.println("Yr doin it wrong!: <Input File> <Output Directory suffix> <#reducers>");
			throw new Exception();
		}
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "MsgData");
	    
	    job.setJarByClass(MsgData.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("MsgData"+args[1]));
	        
	    job.waitForCompletion(true);
	    
	 }
	 }
	        
