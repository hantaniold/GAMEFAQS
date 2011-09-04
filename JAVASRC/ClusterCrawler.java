//ClusterCrawler.java
//A MapReduce application that uses gfCrawl.py on multiple nodes to download
//pages in parallel.

//Note that there is no actual reducer output, it's all junk.

//Also note that it's fairly easy to just use this to run whatever you want on a cluster in parallel,
//provided you have the filepaths set up nicely.
//Sean Hogan, 2011

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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



public class ClusterCrawler {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			context.write(value,new IntWritable(1));
		    
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		//The "key" is just a line of input , e.g. "1-2 /boards/111-game/?page=2 wii" or something.
		//You should have this key be the input to whatever script you're using, in this case gfCrawl.py
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
		//You will want to make the script in a folder all nodes have access to. Fully-qualified pathnames only
	    	String cmd = "/usr/bin/python /home/seanhogan/GAMEFAQS/gfCrawlMR.py "+key.toString(); 

			Runtime run = Runtime.getRuntime();
			Process pr = run.exec(cmd);
			pr.waitFor();
			Scanner scn = new Scanner(key.toString());
			String suffix = scn.next();

			String inputDir = "/home/seanhogan/GAMEFAQS/output/"; //This should be a path that ALL nodes have, containing the output of whatever 
									      //script you are running in parallel.	
			String outputMsgDir = "/user/seanhogan/msgdata/"; //Path on HDFS where message data output goes
			String outputTopicDir = "/user/seanhogan/topicdata/"; //Path on HDFS where topic data output goes

			Configuration conf = new Configuration();
			try { //Hardcoded input streams to message and topic data
				InputStream msgdatain = new BufferedInputStream(new FileInputStream(inputDir+suffix));
				InputStream topicdatain = new BufferedInputStream(new FileInputStream(inputDir+suffix+"topicsData"));
				//Create filesystem objects we can attach an output stream to
				FileSystem msgdata = FileSystem.get(URI.create(outputMsgDir),conf);
				FileSystem topicdata = FileSystem.get(URI.create(outputTopicDir),conf);
				//Make output streams to put data through
				OutputStream msgdataout = msgdata.create(new Path(outputMsgDir+suffix), true, 4096);
				OutputStream topicdataout = topicdata.create(new Path(outputTopicDir+suffix+"topicsData"), true, 4096);
				IOUtils.copyBytes(msgdatain, msgdataout, 4096, true);
				IOUtils.copyBytes(topicdatain, topicdataout, 4096, true);
			
				msgdataout.close();
				msgdatain.close();
				topicdataout.close();
				topicdatain.close();
				
				//Remove the local python output since we don't need it after it's on HDFS.
				run.exec("rm /home/seanhogan/GAMEFAQS/output/"+suffix+" /home/seanhogan/GAMEFAQS/output/"+suffix+"topicsData");
			}
			catch(FileNotFoundException e) {
				
			}
			
			
			context.write(new Text("foo"),new IntWritable(1)); //Junk to write
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "ClusterCrawler");
	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    job.setJarByClass(ClusterCrawler.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("ClusterCrawler"+args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
