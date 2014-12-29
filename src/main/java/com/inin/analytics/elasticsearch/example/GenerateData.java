package com.inin.analytics.elasticsearch.example;

import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class GenerateData {
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid # arguments. EG: [num sample records] [output file]");
			return;
		}

		Long numRecords = new Long(args[0]);
		
		String attribute = null;
		String customer = null;
		String customer1 = UUID.randomUUID().toString();
		String customer2 = UUID.randomUUID().toString();
		
		Configuration config = new Configuration();     
		FileSystem fs = FileSystem.get(config); 
		Path filenamePath = new Path(args[1]);  
		SequenceFile.Writer inputWriter = new SequenceFile.Writer(fs, config, filenamePath, LongWritable.class, Text.class);

		for(Long x = 0l; x < numRecords; x++) {
			if(x % 2 == 0) {
				attribute = "yellow";
			} else {
				attribute = "blue";
			}
			
			if(x % 3 == 0) {
				customer = customer1;
			} else {
				customer = customer2;
			}
			
			inputWriter.append(new LongWritable(x), new Text(customer + "," + UUID.randomUUID().toString() + "," + attribute + "," + RandomStringUtils.randomAlphanumeric(15)));
		}
		
		inputWriter.close();
	}
}
