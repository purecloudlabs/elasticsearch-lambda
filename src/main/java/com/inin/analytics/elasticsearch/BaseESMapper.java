package com.inin.analytics.elasticsearch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class acts as a passthrough routing data to the reducer. Your input should be keyed off of 
 * [index name] | [index type] 
 * 
 * The values should be the raw json payloads to send to ES.
 * @author drew
 *
 */
public class BaseESMapper implements Mapper <Text, Text, Text, Text> {
	public void configure(JobConf job) {
	}

	public void map(Text indexAndRouting, Text documentPayload, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/**
		 * Reducer key looks like this   [indexName]|[routing hash] value [doc type]|[doc id]|json
		 * 
		 */
		output.collect(indexAndRouting, documentPayload);
	} 

	public void close() throws IOException {
	}

}