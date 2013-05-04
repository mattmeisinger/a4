package org.columbia;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

// STEP 2: Compiles the final results and writes to custom-named text files
public class CompileInlinksOutlinks {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		// 'key' is the current position in the file, and 'value' is a line
		// from an input file 
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			// TODO

			output.collect(new Text(''), new Text(''));
		}
	}

	public static class Reduce extends MapReduceBase implements	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			// TODO: Write to custom-named text files for each user and one summary text file
			
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(CompileInlinksOutlinks.class);
		conf.setJobName("Compile Inlinks/Outlinks");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		// Standard input format, since intermediate files can be read line-by-line
		// and do not require the entire file to be taken in context
		conf.setInputFormat(TextInputFormat.class);
		
		// MultiFileOutput is a custom class in which we can define
		// the exact name of the output file that each key will write to.
		conf.setOutputFormat(MultiFileOutput.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);

	}
}
