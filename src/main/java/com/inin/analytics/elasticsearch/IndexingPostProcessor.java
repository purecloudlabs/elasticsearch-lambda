package com.inin.analytics.elasticsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inin.analytics.elasticsearch.transport.BaseTransport;
import com.inin.analytics.elasticsearch.transport.SnapshotTransportStrategy;

public class IndexingPostProcessor {
	private static transient Logger logger = LoggerFactory.getLogger(IndexingPostProcessor.class);
	
	/**
	 * The job output in HDFS is just a manifest of indicies generated by the Job. Why? S3 is eventually consistent in some
     * zones. That means if you try to list the indicies you just generated by this job, you might miss some. Instead, we
     * have the job spit out tiny manifests. This method merges them together, de-dupes them, and if there's any shards that
     * didn't get generated because they have no data it puts a placeholder empty shard in it's place to satisfy ElasticSearch.
     * 
	 * @param jobOutput
	 * @param manifestFile
	 * @param scratchDir
	 * @param shardConfig
	 * @param conf
	 * @param reducerClass
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public void execute(Path jobOutput, Path manifestFile, String scratchDir, ShardConfig shardConfig, Configuration conf, Class<? extends BaseESReducer> reducerClass) throws IOException, InstantiationException, IllegalAccessException {
		FileSystem fs = FileSystem.get(conf);
		ESEmbededContainer esEmbededContainer = null;
		boolean rootManifestUploaded = false;
		try{
			Map<String, Integer> numShardsGenerated = new HashMap<String, Integer>();

			// Each reducer spits out it's own manifest file, merge em all together into 1 file
            FileUtil.copyMerge(fs, jobOutput, fs, manifestFile, false, conf, "");

			// Read the merged file, de-duping entries as it reads
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(manifestFile)));
			String line;
			line=br.readLine();
			Set<String> indicies = new HashSet<>();
			while (line != null){
				indicies.add(line);
				int count = numShardsGenerated.containsKey(line) ? numShardsGenerated.get(line) : 0;
				numShardsGenerated.put(line, count + 1);
				line=br.readLine();
			}

			File scratch = new File(scratchDir);
			if(!scratch.exists()) {
				// Make the dir if it doesn't exist
				scratch.mkdirs();	
			} else {
				FileUtils.deleteDirectory(scratch);
				scratch.mkdirs();
			}
			
			esEmbededContainer = getESEmbededContainer(conf, reducerClass);

			String scratchFile = scratchDir + "manifest";
			PrintWriter writer = new PrintWriter(scratchFile, "UTF-8");
			
			// Create all the indexes
			for(String index : indicies) {
		        esEmbededContainer.getNode().client().admin().indices().prepareCreate(index).setSettings(esEmbededContainer.getDefaultIndexSettings()).get();
			}
			
			// Snapshot it
			List<String> indexesToSnapshot = new ArrayList<>();
			indexesToSnapshot.addAll(indicies);
			esEmbededContainer.snapshot(indexesToSnapshot, BaseESReducer.SNAPSHOT_NAME, conf.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString()), null);

			for(String index : indicies) {
				try{
					placeMissingIndexes(BaseESReducer.SNAPSHOT_NAME, esEmbededContainer, conf, index, shardConfig, !rootManifestUploaded);
					// The root level manifests are the same on each one, so it need only be uploaded once
					rootManifestUploaded = true;
				} catch (FileNotFoundException e) {
					logger.error("Unable to include index " + index + " in the manifest because missing shards could not be generated", e);
					continue;
				}

				// Re-write the manifest to local disk
				writer.println(index);	
			}

			// Clean up index from embedded instance
			for(String index : indicies) {
				esEmbededContainer.getNode().client().admin().indices().prepareDelete(index).execute();
			}

			writer.close();

			// Move the manifest onto HDFS
			fs.copyFromLocalFile(new Path(scratchFile), manifestFile);
		} finally {
			if(esEmbededContainer != null) {
                esEmbededContainer.getNode().close();
				while(!esEmbededContainer.getNode().isClosed());
			}
			FileUtils.deleteDirectory(new File(conf.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString())));
		}
	}

	/**
	 * 
	 * @param snapshotName
	 * @param esEmbededContainer
	 * @param conf
	 * @param index
	 * @param shardConfig
	 * @param includeRootManifest
	 * @throws IOException
	 */
	public void placeMissingIndexes(String snapshotName, ESEmbededContainer esEmbededContainer, Configuration conf, String index, ShardConfig shardConfig, boolean includeRootManifest) throws IOException {
		BaseTransport transport = SnapshotTransportStrategy.get(conf.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()), conf.get(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString()));
		transport.placeMissingShards(snapshotName, index, shardConfig, includeRootManifest);			
	}

	/**
	 * Returns a ESEmbededContainer configured for some local indexing
	 * 
	 * @param conf
	 * @param reducerClass
	 * @return
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private ESEmbededContainer getESEmbededContainer(Configuration conf, Class<? extends BaseESReducer> reducerClass) throws IOException, InstantiationException, IllegalAccessException {
		ESEmbededContainer esEmbededContainer = null;
		
		ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
		.withNodeName("embededESTempLoaderNode")
		.withWorkingDir(conf.get(ConfigParams.ES_WORKING_DIR.toString()))
		.withClusterName("bulkLoadPartition")
		.withSnapshotWorkingLocation(conf.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()))
		.withSnapshotRepoName(conf.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString()))
		.withCustomPlugin("customized_plugin_list");
		
		esEmbededContainer = builder.build();
		
        BaseESReducer red = reducerClass.newInstance();
        if (red.getESVersion() == null) {
            red.setESVersion(esEmbededContainer.getVersion());
        }
        String templateName = red.getTemplateName();
        String templateJson = red.getTemplate();
        red.close();
        
        if (templateName != null && templateJson != null) {
            esEmbededContainer.setTemplate(templateName, templateJson);
        }
		
		return esEmbededContainer;
	}
}
