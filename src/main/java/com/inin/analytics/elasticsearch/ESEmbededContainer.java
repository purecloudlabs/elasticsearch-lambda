package com.inin.analytics.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import com.google.common.base.Preconditions;

/**
 * Builds an embedded elasticsearch instance and configures it for you
 * 
 * @author drew
 *
 */
public class ESEmbededContainer {
	private Node node;
	private long DEFAULT_TIMEOUT_MS = 60 * 5 * 1000; 
	
	public void snapshot(List<String> index, String snapshotName, String snapshotRepoName) {
		snapshot(index, snapshotName, snapshotRepoName, DEFAULT_TIMEOUT_MS);
	}
	
	/**
	 * Flush, optimize, and snapshot an index. Block until complete. 
	 * 
	 * @param index
	 * @param snapshotName
	 * @param snapshotRepoName
	 */
	public void snapshot(List<String> indicies, String snapshotName, String snapshotRepoName, long timeoutMS) {
		/* Flush & optimize before the snapshot.
		 *  
		 * TODO: Long operations could block longer that the container allows an operation to go
		 * unresponsive b/f killing. We need to issue the request and poll the future waiting on the
		 * operation to succeed, but update a counter or something to let the hadoop framework
		 * know the process is still alive. 
		 */  
		TimeValue v = new TimeValue(timeoutMS);
		for(String index : indicies) {
			node.client().admin().indices().prepareFlush(index).get(v);
			node.client().admin().indices().prepareOptimize(index).setWaitForMerge(true).get(v);
		}

		// Snapshot
		node.client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName).setWaitForCompletion(true).setIndices((String[]) indicies.toArray(new String[0])).get();
	}

	public static class Builder {
		private ESEmbededContainer container;
		private String nodeName;
		private Integer numShardsPerIndex;
		private String workingDir;
		private String clusterName;
		private String templateName;
		private String templateSource;
		private String snapshotWorkingLocation;
		private String snapshotRepoName;
		private boolean memoryBackedIndex = false;

		public ESEmbededContainer build() {
			Preconditions.checkNotNull(nodeName);
			Preconditions.checkNotNull(numShardsPerIndex);
			Preconditions.checkNotNull(workingDir);
			Preconditions.checkNotNull(clusterName);

			org.elasticsearch.common.settings.ImmutableSettings.Builder builder = ImmutableSettings.builder()
			.put("http.enabled", false) // Disable HTTP transport, we'll communicate inner-jvm
			.put("processors", 1) // We could experiment ramping this up to match # cores - num reducers per node
			.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShardsPerIndex) 
			.put("node.name", nodeName)
			.put("path.data", workingDir)
			.put("index.refresh_interval", -1) 
			.put("index.translog.flush_threshold_ops", 10000) // Aggressive flushing helps keep the memory footprint below the yarn container max. TODO: Make configurable 
			.put("bootstrap.mlockall", true)
			.put("cluster.routing.allocation.disk.watermark.low", 99) // Nodes don't form a cluster, so routing allocations don't matter
			.put("cluster.routing.allocation.disk.watermark.high", 99)
			.put("index.load_fixed_bitset_filters_eagerly", false)
			.put("indices.fielddata.cache.size", "0%");
			
			if(memoryBackedIndex) {
				builder.put("index.store.type", "memory");
			}
			Settings nodeSettings = builder.build();

			// Create the node
			container.setNode(nodeBuilder()
					.client(false) // It's a client + data node
					.local(true) // Tell ES cluster discovery to be inner-jvm only, disable HTTP based node discovery
					.clusterName(clusterName)
					.settings(nodeSettings)
					.build());

			// Start ES
			container.getNode().start();

			// Configure the cluster with an index template mapping
			if(templateName != null && templateSource != null) {
				container.getNode().client().admin().indices().preparePutTemplate(templateName).setSource(templateSource).get();	
			}

			// Create the snapshot repo
			if(snapshotWorkingLocation != null && snapshotRepoName != null) {
				Map<String, Object> settings = new HashMap<>();
				settings.put("location", snapshotWorkingLocation);
				settings.put("compress", true);
				container.getNode().client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs").setSettings(settings).get();
			}

			return container;
		}

		public Builder() {
			container = new ESEmbededContainer();
		}

		public Builder withNodeName(String nodeName) {
			this.nodeName = nodeName;
			return this;
		}

		public Builder withNumShardsPerIndex(Integer numShardsPerIndex) {
			this.numShardsPerIndex = numShardsPerIndex;
			return this;
		}

		public Builder withWorkingDir(String workingDir) {
			this.workingDir = workingDir;
			return this;
		}
		public Builder withClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		public Builder withTemplate(String templateName, String templateSource) {
			this.templateName = templateName;
			this.templateSource = templateSource;
			return this;
		}

		public Builder withSnapshotWorkingLocation(String snapshotWorkingLocation) {
			this.snapshotWorkingLocation = snapshotWorkingLocation;
			return this;
		}

		public Builder withSnapshotRepoName(String snapshotRepoName) {
			this.snapshotRepoName = snapshotRepoName;
			return this;
		}
		
		public Builder withInMemoryBackedIndexes(boolean memoryBackedIndex) {
			this.memoryBackedIndex = memoryBackedIndex;
			return this;
		}

	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

}
