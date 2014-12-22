package com.inin.analytics.elasticsearch.example;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.index.rotation.RotatedIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;


public class ExampleJobPrep implements Tool {
	private Configuration conf;
	private static final String INDEX_TYPE = "conversation";
	private static final String INDEX_SUFFIX_CONFIG = "indexSuffixConfigKey";
	
	private static final String NUM_SHARDS_PER_CUSTOMER = "numShardsPerCustomer";
	private static final String NUM_SHARDS = "numShards";

	public static class SegmentMapper implements Mapper <LongWritable, Text, Text, Text> {
		private ElasticsearchRoutingStrategy elasticsearchRoutingStrategy;
		
		public void configure(JobConf job) {
			Integer numShardsPerOrg = job.getInt(NUM_SHARDS_PER_CUSTOMER, 1);
			Integer numShards = job.getInt(NUM_SHARDS, 1);
			
			RotatedIndexMetadata indexMetadata = new RotatedIndexMetadata();
			indexMetadata.setNumShards(numShards);
			indexMetadata.setNumShardsPerOrg(numShardsPerOrg);
			elasticsearchRoutingStrategy = new ElasticsearchRoutingStrategyV1();
			elasticsearchRoutingStrategy.configure(indexMetadata);
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] csv = StringUtils.split(value.toString(), ",");
			String docId = csv[1];
			String customerId = csv[0];
			String json = "{\"customerId\":\"" + customerId + "\",\"color\":\"" + csv[2] + "\",\"id\":\"" + docId + "\",\"description\":\"" + csv[3] + "\"}";
			String routingHash = elasticsearchRoutingStrategy.getRoutingHash(customerId, docId);

			output.collect(new Text(INDEX_TYPE + BaseESReducer.TUPLE_SEPARATOR + routingHash), new Text(INDEX_TYPE + BaseESReducer.TUPLE_SEPARATOR + customerId + BaseESReducer.TUPLE_SEPARATOR + json));
		} 

		public void close() throws IOException {
		}
	}

	public int run(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid # arguments. EG: loadES [pipe separated paths to source files containing segments & properties] [output location] [index name suffix] [numShardsPerIndex] [maxNumShardsPerCustomer (for routing)]");
			return -1;
		}
		
		String inputs = args[0];
		String output = args[1];
		String indexSuffix = args[2];
		Integer numShards = new Integer(args[3]);
		Integer numShardsPerCustomer = new Integer(args[4]);
		
		JobConf job = new JobConf(conf, ExampleJobPrep.class);
		job.setJobName("Segment Data Elastic Search Bulk Loader Prep Job");
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setMapperClass(SegmentMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// No reducer required for this example
		job.setNumReduceTasks(0);
		job.set(INDEX_SUFFIX_CONFIG, indexSuffix);
		job.set(NUM_SHARDS_PER_CUSTOMER, numShardsPerCustomer.toString());
		job.set(NUM_SHARDS, numShards.toString());

		FileOutputFormat.setOutputPath(job, new Path(output));

		// Set up inputs
		String[]inputFolders = StringUtils.split(inputs, "|");
		for(String input : inputFolders) {
			FileInputFormat.addInputPath(job, new Path(input));
		}

		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ExampleJobPrep(), args);
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}