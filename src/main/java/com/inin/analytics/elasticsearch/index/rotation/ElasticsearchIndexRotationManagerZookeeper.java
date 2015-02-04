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
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;
import com.inin.analytics.elasticsearch.util.GsonFactory;

public abstract class ElasticsearchIndexRotationManagerZookeeper implements ElasticsearchIndexRotationManager {

	protected abstract String getBaseZnode();
	protected abstract String getRebuildStateZnode();
	
	protected CuratorFramework curator;
	
	protected static transient Logger logger = LoggerFactory.getLogger(ElasticsearchIndexRotationManagerZookeeper.class);
	protected Map<String, NodeCache> indexNameCache = new HashMap<>();
	protected NodeCache rebuildStateCache;
	protected Gson gson = GsonFactory.buildGsonBuilder().create();
	protected static final String FAIL_MESSAGE = "Failed getting routing strategy from zookeeper for ";
	protected Listenable<ConnectionStateListener> connectionStateListener;
	


	public void setCurator(CuratorFramework curator) {
		this.curator = curator;
	}

	public void init() {
		Preconditions.checkNotNull(curator);

		// AR-1785 Create watcher to rebuild nodeCache after ZK reconnects from a connection blip 
		connectionStateListener = curator.getConnectionStateListenable();
		connectionStateListener.addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework arg0, ConnectionState state) {
				if (state.equals(ConnectionState.RECONNECTED) && indexNameCache != null) {
					for(Entry<String, NodeCache> nodeCachePair : indexNameCache.entrySet()) {
						try {
							logger.info("ZK connection reconnect detected, rebuilding curator nodeCache for " + nodeCachePair.getKey());
							nodeCachePair.getValue().rebuild();
						} catch (Exception e) {
							logger.info("Failed to rebuild nodeCache after ZK reconnect ", e);
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
	public void registerIndexAvailableOnRotation(ESIndexMetadata rotatedIndexMetadata) {
		String indexNameZnode = getBaseZnode() + rotatedIndexMetadata.getIndexNameAtBirth();
		try {
			ensureNodePathExists(indexNameZnode);
			// Persisting metadata in json
			curator.setData().forPath(indexNameZnode, gson.toJson(rotatedIndexMetadata).getBytes());
		} catch (Exception e) {
			throw new IllegalStateException("Unable to register znode " + indexNameZnode, e);
		}
	}
	
	public ESIndexMetadata getRotatedIndexMetadata(String indexNameAtBirth) {
		String znode = getBaseZnode() + indexNameAtBirth;
		try {
			if(!indexNameCache.containsKey(indexNameAtBirth)) {
				createNodeCacheForName(znode, indexNameAtBirth);
			}
			ChildData cd = indexNameCache.get(indexNameAtBirth).getCurrentData();
			
			if(cd != null) {
				return gson.fromJson(new String(cd.getData()), ESIndexMetadata.class);
			}
		} catch (Exception e) {
			logger.warn("Error retrieving znode ", e);
		}

		ESIndexMetadata metadata = new ESIndexMetadata();
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
				logger.error("Unable to get pipeline rebuild state", e);
				return null;
			}
		}
		ChildData cd = rebuildStateCache.getCurrentData();
		if(cd != null) {
			return RebuildPipelineState.valueOf(new String(cd.getData()));
		} else {
			return null;
		}
	}


}
