package org.columbia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


// Read through all emails, extract email addresses and compile into a 
// single output directory.
public class ExtractEmailAddresses {
	public static class Map extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> {

		// The 'key' is the current filename and the value is the byte array containing the entire contents of the 
		// file.  Just unencode it using UTF-8 to get the text. 
		public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			reporter.setStatus("Processing input file: " + key);

			String[] lines = new String(value.getBytes(), "UTF-8").trim().split("\\r?\\n");;
			reporter.setStatus("Found lines: " + lines.length);
			
			boolean inFieldTo = false;
			boolean inFieldCc = false;
			boolean toLineEncountered = false;
			boolean ccLineEncountered = false;
			String messageFrom = null;
			List<String> messageTo = new ArrayList<String>();

			for (String line : lines) {
				// Do not process lines that are empty
				if (line == null || line.isEmpty()) {
					continue;
				}
				
				// New field, so it can't be the continuation of a previous field
				if (line.contains(":")) {
					inFieldTo = false;
					inFieldCc = false;
				}
				
				if (line.startsWith("From:") && messageFrom == null) {
					messageFrom = line.replace("From:", "").trim();
					reporter.setStatus("From line: " + line);
				}
				
				if ((line.startsWith("To:") && !toLineEncountered) || inFieldTo) {
					Collections.addAll(messageTo, line.replace("To:", "").trim().split(","));
					reporter.setStatus("To line: " + line);
					toLineEncountered = true;
					inFieldTo = true;
				}
				
				if ((line.startsWith("Cc:") && !ccLineEncountered) || inFieldCc) {
					Collections.addAll(messageTo, line.replace("Cc:", "").trim().split(","));
					reporter.setStatus("Cc line: " + line);
					ccLineEncountered = true;
					inFieldCc = true;
				}
			}
			
			if (messageFrom != null && messageFrom.contains("@")) {
				for (String to : messageTo) {
					
					// do not store email addresses that don't have an '@' sign in them
					if (!to.contains("@")){
						continue;
					}
					
					// write out the two keys
					output.collect(new Text(to.trim()), new Text("Inbound: " + messageFrom.trim()));
					output.collect(new Text(messageFrom.trim()), new Text("Outbound: " + to.trim()));
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			while (values.hasNext()) {
				output.collect(key, new Text(values.next().toString()));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(ExtractEmailAddresses.class);
		conf.setJobName("Extract Email Addresses");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		// Need to use the 'WholeFileInputFormat' so that the files do not get 
		// split up by line, as is the default in Hadoop
		conf.setInputFormat(WholeFileInputFormat.class);
		
		// Standard text output (key values are written to large files, split up into
		// an unknown number of results files).  The output will be cleaned
		// up in the next pass when it goes through the CompileInlinksOutlinks class.
		conf.setOutputFormat(MultipleTextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);

	}
}
