package com.inin.analytics.elasticsearch.example;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.inin.analytics.elasticsearch.BaseESMapper;
import com.inin.analytics.elasticsearch.ConfigParams;
import com.inin.analytics.elasticsearch.IndexingPostProcessor;

public class ExampleIndexingJob implements Tool {

	private static Configuration conf;
	public static int main(String[] args) throws Exception {
		if(args.length != 8) {
			System.err.println("Invalid # arguments. EG: loadES [pipe separated input] [snapshot working directory (fs/nfs)] [snapshot final destination (s3/nfs/hdfs)] [snapshot repo name] [elasticsearch working data location] [num reducers] [num shards per index] [manifest location]");
			return -1;
		}

		String inputPath = args[0];
		String snapshotWorkingLocation = args[1];
		String snapshotFinalDestination = args[2];
		String snapshotRepoName = args[3];
		String esWorkingDir = args[4];
		Integer numReducers = new Integer(args[5]);
		Integer numShardsPerIndex = new Integer(args[6]);
		String manifestLocation = args[7];

		// Remove trailing slashes from the destination 
		snapshotFinalDestination = StringUtils.stripEnd(snapshotFinalDestination, "/");

		conf = new Configuration();
		conf.set(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString(), snapshotWorkingLocation);
		conf.set(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString(), snapshotFinalDestination);
		conf.set(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString(), snapshotRepoName);
		conf.set(ConfigParams.ES_WORKING_DIR.toString(), esWorkingDir);
		conf.set(ConfigParams.NUM_SHARDS_PER_INDEX.toString(), numShardsPerIndex.toString());

		JobConf job = new JobConf(conf, ExampleIndexingJob.class);
		job.setJobName("Elastic Search Offline Index Generator");
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(BaseESMapper.class);
		job.setReducerClass(ExampleIndexingReducerImpl.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setNumReduceTasks(numReducers);
		job.setSpeculativeExecution(false);

		Path jobOutput = new Path(manifestLocation + "/raw/");
		Path manifestFile = new Path(manifestLocation + "manifest");

		FileOutputFormat.setOutputPath(job, jobOutput);

		// Set up inputs
		String[]inputFolders = StringUtils.split(inputPath, "|");
		for(String input : inputFolders) {
			FileInputFormat.addInputPath(job, new Path(input));
		}

		JobClient.runJob(job);
		IndexingPostProcessor postProcessor = new IndexingPostProcessor();
		postProcessor.execute(jobOutput, manifestFile, esWorkingDir, numShardsPerIndex, conf, ExampleIndexingReducerImpl.class);
		return 0;
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
		return ExampleIndexingJob.main(args);
	}

}