package com.inin.analytics.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Builds an embedded elasticsearch instance and configures it for you
 * 
 * @author drew
 *
 */
public class ESEmbededContainer {
	private Node node;
	private long DEFAULT_TIMEOUT_MS = 60 * 30 * 1000; 
	private static Integer MAX_MERGED_SEGMENT_SIZE_MB = 256;
	private Settings defaultIndexSettings;
	private static transient Logger logger = LoggerFactory.getLogger(ESEmbededContainer.class);
	
	public void snapshot(List<String> index, String snapshotName, String snapshotRepoName, Reporter reporter) {
		snapshot(index, snapshotName, snapshotRepoName, DEFAULT_TIMEOUT_MS, reporter);
	}
	
	/**
	 * Flush, optimize, and snapshot an index. Block until complete. 
	 * 
	 * @param index
	 * @param snapshotName
	 * @param snapshotRepoName
	 */
	public void snapshot(List<String> indicies, String snapshotName, String snapshotRepoName, long timeoutMS, Reporter reporter) {
		/* Flush & optimize before the snapshot.
		 *  
		 * TODO: Long operations could block longer that the container allows an operation to go
		 * unresponsive b/f killing. We need to issue the request and poll the future waiting on the
		 * operation to succeed, but update a counter or something to let the hadoop framework
		 * know the process is still alive. 
		 */  
		TimeValue v = new TimeValue(timeoutMS);
		for(String index : indicies) {
			long start = System.currentTimeMillis();

			// Flush
			node.client().admin().indices().prepareFlush(index).get(v);
			if(reporter != null) {
				reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_FLUSHING_MS, System.currentTimeMillis() - start);
			}

			// Merge
			start = System.currentTimeMillis();
			node.client().admin().indices().prepareForceMerge(index).get(v);
			if(reporter != null) {
				reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_MERGING_MS, System.currentTimeMillis() - start);
			}
		}

		// Snapshot
		long start = System.currentTimeMillis();
	    node.client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName).setIndices((String[]) indicies.toArray(new String[0])).execute();
        
		// ES snapshot restore ignores timers and will block no more than 30s :( You have to block & poll to make sure it's done
		blockForSnapshot(snapshotRepoName, indicies, timeoutMS);	
		
		if(reporter != null) {
			reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_SNAPSHOTTING_MS, System.currentTimeMillis() - start);
		}
	}

	/** 
	 * Block for index snapshots to be complete
	 *  
	 * @param snapshotRepoName
	 * @param index
	 * @param timeoutMS
	 * @param reporter
	 */
	private void blockForSnapshot(String snapshotRepoName, List<String> indicies, long timeoutMS) {
		long start = System.currentTimeMillis();
		while(System.currentTimeMillis() - start < timeoutMS) {

			GetSnapshotsResponse repos = node.client().admin().cluster().getSnapshots(new GetSnapshotsRequest(snapshotRepoName)).actionGet();
				for(SnapshotInfo i : repos.getSnapshots()) {
					if(i.state().completed() && i.successfulShards() == i.totalShards() && i.totalShards() >= indicies.size()) {
						logger.info("Snapshot completed {} out of {} indicies. Snapshot state {}. ", i.successfulShards(), i.totalShards(), i.state().completed());
						return;
					} else {
						logger.info("Snapshotted {} out of {} indicies, polling for completion. Snapshot state {}.", i.successfulShards(), i.totalShards(), i.state().completed());
					}
				}
			try {
				// Don't slam ES with snapshot status requests in a tight loop
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	public void deleteSnapshot(String snapshotName, String snapshotRepoName) {
		node.client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).execute().actionGet();
	}

	private static class PluginConfigurableNode extends Node {
        public PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
        }
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
		private String customPluginListFile;

		public ESEmbededContainer build() {
			Preconditions.checkNotNull(nodeName);
			Preconditions.checkNotNull(numShardsPerIndex);
			Preconditions.checkNotNull(workingDir);
			Preconditions.checkNotNull(clusterName);

			org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder()
			.put("http.enabled", false) // Disable HTTP transport, we'll communicate inner-jvm
			.put("processors", 1) // We could experiment ramping this up to match # cores - num reducers per node
			.put("node.name", nodeName)
			.put("path.data", workingDir)
            .put("path.repo", snapshotWorkingLocation)
            .put("path.home", "/tmp")
            .put("bootstrap.memory_lock", true)
			.put("cluster.routing.allocation.disk.watermark.low", "99%") // Nodes don't form a cluster, so routing allocations don't matter
			.put("cluster.routing.allocation.disk.watermark.high", "99%")
            .put("node.data", true)       //data node
            .put("transport.type", "local")
            .put("indices.memory.index_buffer_size", "5%") // The default 10% is a bit large b/c it's calculated against JVM heap size & not Yarn container allocation. Choosing a good value here could be made smarter.
            .put("indices.fielddata.cache.size", "0%")
            .put("cluster.name", clusterName);
			
			Settings nodeSettings = builder.build();

			try {
                //read plugin list
                ArrayList<Class<?>> pluginClasses = new ArrayList<Class<?>>();
                if (customPluginListFile != null) {
                    ClassLoader classloader = this.getClass().getClassLoader();
                    InputStream is = classloader.getResourceAsStream(customPluginListFile);
                    if (is != null) {
                        String deliminator = ";";
                        String pluginlist = getStringFromInputStream(is, deliminator);
                        if (pluginlist.isEmpty()) {
                            logger.error("Plugins should be separated by ;");
                        } else {
                            String[] pluginClassnames = pluginlist.split(deliminator);
                            for (String pluginClassname: pluginClassnames) {
                                logger.info("Plugin "+pluginClassname+" is loaded.");
                                try {
                                    Class<?> pluginClazz = Class.forName(pluginClassname);
                                    if (pluginClazz.newInstance() instanceof Plugin) {
                                        pluginClasses.add(pluginClazz);
                                    }
                                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                                    logger.error("Error in getting plugin classes, {}.", pluginClassname, e);
                                }
                            }
                        }
                    } else {
                        logger.info("Not found the custom plugin list. No plugins are loaded.");
                    }
                }

                // Create the node
                Collection plugins = pluginClasses;
                container.setNode(new PluginConfigurableNode(nodeSettings, plugins));

	            // Start ES
                container.getNode().start();

                // Configure the cluster with an index template mapping
    			if(templateName != null && templateSource != null) {
    			    org.elasticsearch.common.settings.Settings.Builder indexBuilder = Settings.builder()
    			            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShardsPerIndex) 
    			            .put("index.refresh_interval", -1) 
    			            .put("index.translog.flush_threshold_size", "128mb") // Aggressive flushing helps keep the memory footprint below the yarn container max. TODO: Make configurable 
    			            .put("index.load_fixed_bitset_filters_eagerly", false)
    			            .put("index.merge.policy.max_merged_segment", MAX_MERGED_SEGMENT_SIZE_MB + "mb") // The default 5gb segment max size is too large for the typical hadoop node
    			            .put("index.merge.policy.max_merge_at_once", 10) 
    			            .put("index.merge.policy.segments_per_tier", 4)
    			            .put("index.merge.scheduler.max_thread_count", 1)
    			            .put("index.compound_format", false) // Explicitly disable compound files
    			            .put("index.codec", "best_compression"); // Lucene 5/ES 2.0 feature to play with when that's out

                     container.setDefaultIndexSettings(indexBuilder.build());
    		         container.getNode().client().admin().indices().preparePutTemplate(templateName).setSource(templateSource).get();
    			}

    			// Create the snapshot repo
    			if(snapshotWorkingLocation != null && snapshotRepoName != null) {
    				Map<String, Object> settings = new HashMap<>();
    				settings.put("location", snapshotWorkingLocation);
    				settings.put("compress", true);
    				settings.put("max_snapshot_bytes_per_sec", "400mb"); // The default 20mb/sec is very slow for a local disk to disk snapshot
    				container.getNode().client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs").setSettings(settings).get();
    			}
            } catch (NodeValidationException e) {
                logger.error("Error in getting ES node. ", e);
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
		
		public Builder withCustomPlugin(String customPluginListFile) {
		    this.customPluginListFile = customPluginListFile;
		    return this;
		}
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

    public void setDefaultIndexSettings(Settings defaultIndexSettings) {
        this.defaultIndexSettings = defaultIndexSettings;
    }

    public Settings getDefaultIndexSettings() {
	    return defaultIndexSettings;
	}

	private static String getStringFromInputStream(InputStream is, String deliminator) {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line+deliminator);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }
}
