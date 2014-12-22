package com.inin.analytics.elasticsearch;

import java.io.File;
import java.io.IOException;
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
	
	// We prefix all snapshots with the word snapshot
	private static final String SNAPSHOT_NAME_PREFIX = "snapshot";
	
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
		esEmbededContainer.getNode().client().admin().indices().prepareCreate(index).get();
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
			String indexType = pieces[0];
			String docId = pieces[1];
			String pre = indexType + TUPLE_SEPARATOR + docId + TUPLE_SEPARATOR;
			String json = line.toString().substring(pre.length());

			bulkRequestBuilder.add(esEmbededContainer.getNode().client().prepareIndex(indexName, indexType).setId(docId).setRouting(routing).setSource(json));
			
			if(count % esBatchCommitSize == 0) {
				bulkRequestBuilder.execute().actionGet();
				bulkRequestBuilder = esEmbededContainer.getNode().client().prepareBulk();
				count = 0;
			}
		}
		if(count > 0) {
			bulkRequestBuilder.execute().actionGet();		
		}
		
		snapshot(indexName);
		output.collect(NullWritable.get(), new Text(indexName));
	}

	public void close() throws IOException {
		
	}
	
	public void snapshot(String index) throws IOException {
		String snapshotName = SNAPSHOT_NAME_PREFIX + index;
		esEmbededContainer.snapshot(index, snapshotName, snapshotRepoName);
		esEmbededContainer.getNode().close();
		
		// Cleanup the working dir
		FileUtils.deleteDirectory(new File(esWorkingDir));
		
		// Move the shard snapshot to the destination
		SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination).execute(snapshotName, index);

		// Cleanup the snapshot dir
		FileUtils.deleteDirectory(new File(snapshotWorkingLocation));
	}
}