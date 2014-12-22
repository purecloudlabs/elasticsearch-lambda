package com.inin.analytics.elasticsearch.example;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.index.rotation.RotatedIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;

/**
 * Sample hadoop job for taking data from GenerateData.java and writes it out
 * into a format suitable for ExampleIndexingJob.java 
 */
public class ExampleJobPrep  implements Tool {
	private static Configuration conf;
	private static final String INDEX_TYPE = "conversation";
	private static final String INDEX_SUFFIX_CONFIG = "indexSuffixConfigKey";

	private static final String NUM_SHARDS_PER_CUSTOMER = "numShardsPerCustomer";
	private static final String NUM_SHARDS = "numShards";

	public static class DocMapper extends Mapper <LongWritable, Text, Text, Text> {
		private ElasticsearchRoutingStrategy elasticsearchRoutingStrategy;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Integer numShardsPerOrg = context.getConfiguration().getInt(NUM_SHARDS_PER_CUSTOMER, 1);
			Integer numShards = context.getConfiguration().getInt(NUM_SHARDS, 1);

			RotatedIndexMetadata indexMetadata = new RotatedIndexMetadata();
			indexMetadata.setNumShards(numShards);
			indexMetadata.setNumShardsPerOrg(numShardsPerOrg);
			elasticsearchRoutingStrategy = new ElasticsearchRoutingStrategyV1();
			elasticsearchRoutingStrategy.configure(indexMetadata);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] csv = StringUtils.split(value.toString(), ",");
			String docId = csv[1];
			String customerId = csv[0];
			String json = "{\"customerId\":\"" + customerId + "\",\"color\":\"" + csv[2] + "\",\"id\":\"" + docId + "\",\"description\":\"" + csv[3] + "\"}";
			String routingHash = elasticsearchRoutingStrategy.getRoutingHash(customerId, docId);

			Text outputKey = new Text(INDEX_TYPE + BaseESReducer.TUPLE_SEPARATOR + routingHash);
			Text outputValue = new Text(INDEX_TYPE + BaseESReducer.TUPLE_SEPARATOR + customerId + BaseESReducer.TUPLE_SEPARATOR + json);
			context.write(outputKey, outputValue);
		} 
	}

	public static boolean main(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid # arguments. EG: loadES [pipe separated paths to source files containing segments & properties] [output location] [index name suffix] [numShardsPerIndex] [maxNumShardsPerCustomer (for routing)]");
		}

		String inputs = args[0];
		String output = args[1];
		String indexSuffix = args[2];
		Integer numShards = new Integer(args[3]);
		Integer numShardsPerCustomer = new Integer(args[4]);

		conf = new Configuration();
		Job job = Job.getInstance(conf, "Prep example");
		job.setJarByClass(ExampleJobPrep.class);
		job.setMapperClass(DocMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(0);

		job.getConfiguration().set(INDEX_SUFFIX_CONFIG, indexSuffix);
		job.getConfiguration().set(NUM_SHARDS_PER_CUSTOMER, numShardsPerCustomer.toString());
		job.getConfiguration().set(NUM_SHARDS, numShards.toString());

		FileOutputFormat.setOutputPath(job, new Path(output));

		// Set up inputs
		String[]inputFolders = StringUtils.split(inputs, "|");
		for(String input : inputFolders) {
			FileInputFormat.addInputPath(job, new Path(input));	
		}

		return job.waitForCompletion(true);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean success = ExampleJobPrep.main(args);
		if(success) {
			return 0;
		} else {
			return 1;
		}
	}

}