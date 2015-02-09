package com.inin.analytics.elasticsearch.index.rotation;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.inin.analytics.elasticsearch.util.GsonFactory;

public abstract class ElasticsearchIndexRotationManagerZookeeper implements ElasticsearchIndexRotationManager {

	protected abstract String getBaseZnode();
	protected abstract String getRebuildStateZnode();
	
	protected CuratorFramework curator;
	
	protected static transient Logger logger = LoggerFactory.getLogger(ElasticsearchIndexRotationManagerZookeeper.class);
	protected Map<String, NodeCache> indexNameCache = new HashMap<>();
	protected NodeCache rebuildStateCache;
	protected Gson gson;
	protected static final String FAIL_MESSAGE = "Failed getting routing strategy from zookeeper for ";
	protected Listenable<ConnectionStateListener> connectionStateListener;
	


	public void setCurator(CuratorFramework curator) {
		this.curator = curator;
	}

	public void init() {
		Preconditions.checkNotNull(curator, "curator is a required dependency");
		gson = GsonFactory.buildGsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

		// AR-1785 Create watcher to rebuild nodeCache after ZK reconnects from a connection blip 
		connectionStateListener = curator.getConnectionStateListenable();
		connectionStateListener.addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework curator, ConnectionState state) {
				if (state.equals(ConnectionState.RECONNECTED) && indexNameCache != null) {
					for(Entry<String, NodeCache> nodeCachePair : indexNameCache.entrySet()) {
						try {
							// nodeCache stops updating after a connection blip nukes its listener. I'd almost consider that a bug in curator, but for now this is the advised workaround.  
							logger.info("ZK connection reconnect detected, rebuilding curator nodeCache for " + nodeCachePair.getKey());
							nodeCachePair.getValue().rebuild();
						} catch (Exception e) {
							logger.warn("Failed to rebuild nodeCache after ZK reconnect ", e);
						}
					}
				}
			}
		});
	}

	protected void ensureNodePathExists(String zkPath) throws Exception {
		try {
			String[] pieces = StringUtils.split(zkPath, "/");
			String znode = "";
			for(String piece : pieces) {
				znode = znode + "/" + piece;
				try {
					curator.getData().forPath(znode);
				} catch (KeeperException e) {
					Code errorCode = e.code();
					if(errorCode.equals(Code.NONODE)) {
						curator.create().forPath(znode);
					}
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to initialize ElasticsearchIndexRotationStrategy because base ZK nodes could not be created", e);
		}
	}


	/**
	 * Register that an index is on the hadoop rebuild train. We'll store the index name that the live data
	 * would normally flow into with the alias to the rebuilt index. EG
	 * 
	 * New index at birth looks like
	 * c140101
	 * 
	 * Rebuilt index 
	 * c140101_reubild_82389238923
	 * 
	 * Rebuilt index alias 
	 * c140101_r
	 * 
	 * @param bucket
	 */
	
	@Override
	public void registerIndexAvailableOnRotation(ElasticSearchIndexMetadata rotatedIndexMetadata) {
		String indexNameZnode = getBaseZnode() + rotatedIndexMetadata.getIndexNameAtBirth();
		try {
			ensureNodePathExists(indexNameZnode);
			// Persisting metadata in json
			curator.setData().forPath(indexNameZnode, gson.toJson(rotatedIndexMetadata).getBytes());
		} catch (Exception e) {
			throw new IllegalStateException("Unable to register znode " + indexNameZnode, e);
		}
	}
	
	public ElasticSearchIndexMetadata getElasticSearchIndexMetadata(String indexNameAtBirth) {
		String znode = getBaseZnode() + indexNameAtBirth;
		try {
			if(!indexNameCache.containsKey(indexNameAtBirth)) {
				createNodeCacheForName(znode, indexNameAtBirth);
			}
			ChildData cd = indexNameCache.get(indexNameAtBirth).getCurrentData();
			
			if(cd != null) {
				String json = new String(cd.getData());
				return gson.fromJson(json, ElasticSearchIndexMetadata.class);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error retrieving znode, unable to maintain index metadata ", e);
		}

		ElasticSearchIndexMetadata metadata = new ElasticSearchIndexMetadata();
		metadata.setIndexNameAtBirth(indexNameAtBirth);
		return metadata;
	}
	

	protected synchronized void createNodeCacheForName(String zkPath, final String indexName) throws Exception {
		final NodeCache nodeCache = new NodeCache(curator, zkPath);
		nodeCache.start(true);
		indexNameCache.put(indexName, nodeCache);
	}

	@Override
	public void updateRebuildPipelineState(RebuildPipelineState state) {
		try {
			ensureNodePathExists(getRebuildStateZnode());
			// Persisting metadata in json
			curator.setData().forPath(getRebuildStateZnode(), state.name().getBytes());
		} catch (Exception e) {
			throw new IllegalStateException("Unable to register state " + state, e);
		}
	}

	@Override
	public RebuildPipelineState getRebuildPipelineState() {
		if(rebuildStateCache == null) {
			rebuildStateCache = new NodeCache(curator, getRebuildStateZnode());
			try {
				rebuildStateCache.start(true);
			} catch (Exception e) {
				throw new IllegalStateException("Unable to get pipeline rebuild state", e);
			}
		}
		ChildData cd = rebuildStateCache.getCurrentData();
		if(cd != null) {
			return RebuildPipelineState.valueOf(new String(cd.getData()));
		} else {
			// COMPLETE ~= NOT_RUNNING, so if it's never been ran that's what we default to. At some point we'll want to fix the enum.
			return RebuildPipelineState.COMPLETE;
		}
	}


}
