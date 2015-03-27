package com.inin.analytics.elasticsearch;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;

import com.inin.analytics.elasticsearch.transport.SnapshotTransportStrategy;

public abstract class BaseESReducer implements Reducer<Text, Text, NullWritable, Text> {
	public static final char TUPLE_SEPARATOR = '|';
	public static final char DIR_SEPARATOR = '/';
	public static enum JOB_COUNTER {
		TIME_SPENT_INDEXING_MS, TIME_SPENT_FLUSHING_MS, TIME_SPENT_MERGING_MS, TIME_SPENT_SNAPSHOTTING_MS, TIME_SPENT_TRANSPORTING_SNAPSHOT_MS
	}
	
	// We prefix all snapshots with the word snapshot
	public static final String SNAPSHOT_NAME = "snapshot";
	
	// The local filesystem location that ES will write the snapshot out to
	private String snapshotWorkingLocation;
	
	// Where the snapshot will be moved to. Typical use case would be to throw it onto S3
	private String snapshotFinalDestination;
	
	// The name of a snapshot repo. We'll enumerate that on each job run so that the repo names are unique across rebuilds
	private String snapshotRepoName;
	
	// Local filesystem location where index data is built
	private String esWorkingDir;
	
	// Despite cutting out HTTP we'll still batch the writes to ES 
	private Integer esBatchCommitSize;
	
	// The partition of data this reducer is serving. Useful for making directories unique if running multiple reducers on a task tracker 
	private String partition;
	
	// How many shards are in an index
	private Integer numShardsPerIndex;
	
	// The container handles spinning up our embedded elasticsearch instance
	private ESEmbededContainer esEmbededContainer;
	
	// Hold onto some frequently generated objects to cut down on GC overhead 
	private String indexType;
	private String docId;
	private String pre;
	private String json;
	private String snapshotName;
   
	public void configure(JobConf job) {
		partition = job.get("mapred.task.partition");
        String attemptId = job.get("mapred.task.id");
		
		// If running multiple reducers on a node, the node needs a unique name & data directory hence the random number we append 
		snapshotWorkingLocation = job.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()) + partition + attemptId + DIR_SEPARATOR;
		snapshotFinalDestination = job.get(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString());
		snapshotRepoName = job.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString());
		esWorkingDir = job.get(ConfigParams.ES_WORKING_DIR.toString()) + partition + attemptId + DIR_SEPARATOR;
		numShardsPerIndex = new Integer(job.get(ConfigParams.NUM_SHARDS_PER_INDEX.toString()));
		esBatchCommitSize = new Integer(job.get(ConfigParams.ES_BATCH_COMMIT_SIZE.toString()));
	}
	

	private void init(String index) {
		String templateName = getTemplateName();
		String templateJson = getTemplate();

		ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
		.withNodeName("embededESTempLoaderNode" + partition)
		.withWorkingDir(esWorkingDir)
		.withClusterName("bulkLoadPartition:" + partition)
		.withNumShardsPerIndex(numShardsPerIndex)
		.withSnapshotWorkingLocation(snapshotWorkingLocation)
		.withSnapshotRepoName(snapshotRepoName);
		
		if(templateName != null && templateJson != null) {
			builder.withTemplate(templateName, templateJson);	
		}
		
		esEmbededContainer = builder.build();
		
		// Create index
		esEmbededContainer.getNode().client().admin().indices().prepareCreate(index).setSettings(settingsBuilder().put("index.number_of_replicas", 0)).get();
	}
	
	/**
	 * Provide the JSON contents of the index template. This is your hook for configuring ElasticSearch.
	 * 
	 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-templates.html
	 */
	public abstract String getTemplate();
	
	/**
	 * Provide an all lower case template name
	 *  
	 * @return
	 */
	public abstract String getTemplateName();

	public void reduce(Text docMetaData, Iterator<Text> documentPayloads, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
		String[] pieces = StringUtils.split(docMetaData.toString(), TUPLE_SEPARATOR);
		String indexName = pieces[0];
		String routing = pieces[1]; 
		init(indexName);

		BulkRequestBuilder bulkRequestBuilder = esEmbededContainer.getNode().client().prepareBulk();
		int count = 0;
		while(documentPayloads.hasNext()) {
			count++;
			Text line = documentPayloads.next();
			if(line == null) {
				continue;
			}
			
			pieces = StringUtils.split(line.toString(), TUPLE_SEPARATOR);
			indexType = pieces[0];
			docId = pieces[1];
			pre = indexType + TUPLE_SEPARATOR + docId + TUPLE_SEPARATOR;
			json = line.toString().substring(pre.length());

			bulkRequestBuilder.add(esEmbededContainer.getNode().client().prepareIndex(indexName, indexType).setId(docId).setRouting(routing).setSource(json));
			
			if(count % esBatchCommitSize == 0) {
				bulkRequestBuilder.execute().actionGet();
				bulkRequestBuilder = esEmbededContainer.getNode().client().prepareBulk();
				count = 0;
			}
		}
		if(count > 0) {
			long start = System.currentTimeMillis();
			bulkRequestBuilder.execute().actionGet();
			reporter.incrCounter(JOB_COUNTER.TIME_SPENT_INDEXING_MS, System.currentTimeMillis() - start);
		}
		
		snapshot(indexName, reporter);
		output.collect(NullWritable.get(), new Text(indexName));
	}

	public void close() throws IOException {
		
	}
	
	public void snapshot(String index, Reporter reporter) throws IOException {
		esEmbededContainer.snapshot(Arrays.asList(index), SNAPSHOT_NAME, snapshotRepoName, reporter);
		esEmbededContainer.getNode().close();
		while(!esEmbededContainer.getNode().isClosed());
		esEmbededContainer = null;
		System.gc();
		
		// Cleanup the working dir
		FileUtils.deleteDirectory(new File(esWorkingDir));
		
		// Move the shard snapshot to the destination
		long start = System.currentTimeMillis();
		SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination).execute(SNAPSHOT_NAME, index);
		reporter.incrCounter(JOB_COUNTER.TIME_SPENT_TRANSPORTING_SNAPSHOT_MS, System.currentTimeMillis() - start);
		
		// Cleanup the snapshot dir
		FileUtils.deleteDirectory(new File(snapshotWorkingLocation));
	}
}