package com.inin.analytics.elasticsearch;

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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;
import com.inin.analytics.elasticsearch.transport.SnapshotTransportStrategy;

public abstract class BaseESReducer implements Reducer<Text, Text, NullWritable, Text> {
	public static final char TUPLE_SEPARATOR = '|';
	public static final char DIR_SEPARATOR = '/';
	
	public static enum JOB_COUNTER {
		TIME_SPENT_INDEXING_MS, TIME_SPENT_FLUSHING_MS, TIME_SPENT_MERGING_MS, TIME_SPENT_SNAPSHOTTING_MS, TIME_SPENT_TRANSPORTING_SNAPSHOT_MS, INDEXING_DOC_FAIL, INDEX_DOC_CREATED, INDEX_DOC_NOT_CREATED
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
	
	// The partition of data this reducer is serving. Useful for making directories unique if running multiple reducers on a task tracker 
	private String partition;
	
	// The container handles spinning up our embedded elasticsearch instance
	private ESEmbededContainer esEmbededContainer;
	
	private String esVersion;
	
	private ShardConfig shardConfig;
		
	// Hold onto some frequently generated objects to cut down on GC overhead 
	private String indexType;
	private String docId;
	private String pre;
	private String json;
   
	@Override
	public void configure(JobConf job) {
		partition = job.get("mapred.task.partition");
        String attemptId = job.get("mapred.task.id");
		
		// If running multiple reducers on a node, the node needs a unique name & data directory hence the random number we append 
		snapshotWorkingLocation = job.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()) + partition + attemptId + DIR_SEPARATOR;
		snapshotFinalDestination = job.get(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString());
		snapshotRepoName = job.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString());
		esWorkingDir = job.get(ConfigParams.ES_WORKING_DIR.toString()) + partition + attemptId + DIR_SEPARATOR;
		if(shardConfig == null) {
		    shardConfig = getShardConfig(job);    
		}
	}
	
	public void setShardConfig(ShardConfig shardConfig) {
        this.shardConfig = shardConfig;
    }

    private void init(String index) {
		ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
		.withNodeName("embededESTempLoaderNode" + partition)
		.withWorkingDir(esWorkingDir)
		.withClusterName("bulkLoadPartition:" + partition)
		.withSnapshotWorkingLocation(snapshotWorkingLocation)
		.withSnapshotRepoName(snapshotRepoName)
		.withCustomPlugin("customized_plugin_list");
		
		if(esEmbededContainer == null) {
			esEmbededContainer = builder.build();	
		} 

		setESVersion(esEmbededContainer.getVersion());
		
		// Put template after building esEmbededContainer but before creating index
		String templateName = getTemplateName();
        String templateJson = getTemplate();
        
        if (templateName != null && templateJson != null) {
            esEmbededContainer.setTemplate(templateName, templateJson);
        }

		// Create index
        esEmbededContainer.getNode().client().admin().indices().prepareCreate(index).setSettings(esEmbededContainer.getDefaultIndexSettings().put("index.number_of_replicas", 0)).get();
	}
	
	/**
	 * Provide the JSON contents of the index template. This is your hook for configuring ElasticSearch.
	 * 
	 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-templates.html
	 * 
     * @return String
	 */
	public abstract String getTemplate();
	
	
	/**
	 * Provide a ShardConfig which provides the number of shards per index and the number of 
	 * shards to split organizations across. The number can be uniform across indices or a mapping
	 * can be provided to enable per-index configuration values.
	 *  
	 * @param job
	 * @return ShardConfig
	 */
	public abstract ShardConfig getShardConfig(JobConf job);
	
	/**
	 * Provide an all lower case template name
	 *  
	 * @return String
	 */
	public abstract String getTemplateName();

	@Override
	public void reduce(Text docMetaData, Iterator<Text> documentPayloads, OutputCollector<NullWritable, Text> output, final Reporter reporter) throws IOException {
		String[] pieces = StringUtils.split(docMetaData.toString(), TUPLE_SEPARATOR);
		String indexName = pieces[0];
		String routing = pieces[1]; 
		init(indexName);

		long start = System.currentTimeMillis();
		while(documentPayloads.hasNext()) {
			Text line = documentPayloads.next();
			if(line == null) {
				continue;
			}
			
			pieces = StringUtils.split(line.toString(), TUPLE_SEPARATOR);
			indexType = pieces[0];
			docId = pieces[1];
			pre = indexType + TUPLE_SEPARATOR + docId + TUPLE_SEPARATOR;
			json = line.toString().substring(pre.length());

			IndexResponse response = esEmbededContainer.getNode().client().prepareIndex(indexName, indexType).setId(docId).setRouting(routing).setSource(json).execute().actionGet();
			if(response.status() == RestStatus.CREATED) {
				reporter.incrCounter(JOB_COUNTER.INDEX_DOC_CREATED, 1l);
			} else {
				reporter.incrCounter(JOB_COUNTER.INDEX_DOC_NOT_CREATED, 1l);
			}
		}

		reporter.incrCounter(JOB_COUNTER.TIME_SPENT_INDEXING_MS, System.currentTimeMillis() - start);
        
		snapshot(indexName, reporter);
		output.collect(NullWritable.get(), new Text(indexName));
	}

	@Override
	public void close() throws IOException {
		if(esEmbededContainer != null) {
			esEmbededContainer.getNode().close();
			while(!esEmbededContainer.getNode().isClosed());
			FileUtils.deleteDirectory(new File(snapshotWorkingLocation));
		}
	}

	public void snapshot(String index, Reporter reporter) throws IOException {
	    esEmbededContainer.snapshot(Arrays.asList(index), SNAPSHOT_NAME, snapshotRepoName, reporter);

		// Delete the index to free up that space
		ActionFuture<DeleteIndexResponse> response = esEmbededContainer.getNode().client().admin().indices().delete(new DeleteIndexRequest(index));
		while(!response.isDone());
		
		// Move the shard snapshot to the destination
		long start = System.currentTimeMillis();
		SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination).execute(SNAPSHOT_NAME);
        reporter.incrCounter(JOB_COUNTER.TIME_SPENT_TRANSPORTING_SNAPSHOT_MS, System.currentTimeMillis() - start);
		
		esEmbededContainer.deleteSnapshot(SNAPSHOT_NAME, snapshotRepoName);
	}

	public String getESVersion() {
	    return esVersion;
	}
	
	public void setESVersion(String esVersion) {
	    this.esVersion = esVersion;
	}
}