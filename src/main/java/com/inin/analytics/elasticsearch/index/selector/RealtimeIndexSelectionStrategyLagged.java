package com.inin.analytics.elasticsearch.index.selector;

import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inin.analytics.elasticsearch.index.rotation.ESIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;


/**
 * For a realtime engine feeding data into elasticsearch or pulling data out, some strategies
 * around which index to use are employed here. For example, we know if live data is streaming
 * into today's index that we may want to ignore today's index that got rebuilt in hadoop because
 * the data invariably changed from the second the job started. This employs an x day lag on
 * switching over reads/writes to hadoop generated indexes.
 *  
 * @author drew
 *
 */
public class RealtimeIndexSelectionStrategyLagged implements RealtimeIndexSelectionStrategy {

	private int LAG = 2;
	protected static final String FAIL_MESSAGE = "Failed getting routing strategy from zookeeper for ";
	private static transient Logger logger = LoggerFactory.getLogger(RealtimeIndexSelectionStrategyLagged.class);
	

	/**
	 * 
	 * @param lAG
	 */
	public RealtimeIndexSelectionStrategyLagged(int lAG) {
		super();
		LAG = lAG;
	}

	public ElasticsearchRoutingStrategy get(ESIndexMetadata rotatedIndexMetadata) {
		
		DateTime now = new DateTime();
		if(rotatedIndexMetadata != null && rotatedIndexMetadata.getRoutingStrategyClassName() != null && !rotatedIndexMetadata.getDate().isAfter(now.minusDays(LAG).toLocalDate())) {
			ElasticsearchRoutingStrategy strategy;
			try {
				strategy = (ElasticsearchRoutingStrategy) Class.forName(rotatedIndexMetadata.getRoutingStrategyClassName(), true, ClassLoader.getSystemClassLoader()).newInstance();
				strategy.configure(rotatedIndexMetadata);
				return strategy;
			} catch (InstantiationException e) {
				logger.error(FAIL_MESSAGE + rotatedIndexMetadata, e);
			} catch (IllegalAccessException e) {
				logger.error(FAIL_MESSAGE + rotatedIndexMetadata, e);
			} catch (ClassNotFoundException e) {
				logger.error(FAIL_MESSAGE + rotatedIndexMetadata, e);
			}
		}
		return null;
	}
	
	/**
	 * Check if the routing strategy is the same for all the indicies. This is useful 
	 * when searching because all indexes have to have the same routing strategy in order to 
	 * cut down the # of shards to search. If they differ, then you have to hit the whole index.
	 * 
	 * Returns null if there's no common routing strategy
	 * 
	 * @param indices
	 * @return
	 */
	public ElasticsearchRoutingStrategy getRoutingStrategyForIndicies(List<ESIndexMetadata> indices) {
		ElasticsearchRoutingStrategy routingStrategy = null;
		for(ESIndexMetadata index : indices) {
			if(index.getRoutingStrategyClassName() == null) {
				// If the routing strategy isn't set, then there can be no common strategy
				routingStrategy = null;
				break;
			} else if(routingStrategy == null) {
				routingStrategy = get(index);
			} else {
				ElasticsearchRoutingStrategy routingStrategy2 = get(index);
				if(routingStrategy2 == null || !routingStrategy2.equals(routingStrategy)) {
					routingStrategy = null;
					break;
				}
			}
		}
		return routingStrategy;
	}

	public String getIndexWritable(ESIndexMetadata rotatedIndexMetadata) {
		DateTime now = new DateTime();
		if(rotatedIndexMetadata.getRebuiltIndexName() == null || (rotatedIndexMetadata.getDate() != null && rotatedIndexMetadata.getDate().isAfter(now.minusDays(LAG).toLocalDate()))) {
			// Only use rotated indexes for data that's ROTATION_LAG_DAYS old
			return rotatedIndexMetadata.getIndexNameAtBirth();
		}
		
		return rotatedIndexMetadata.getRebuiltIndexName();
	}

	/**
	 * When reading from ES, searches may hit many indexes. To help avoid hitting the URL size limit
	 * when doing searches that hit a large # of indexes, we alias them with something short. 
	 */
	public String getIndexReadable(ESIndexMetadata rotatedIndexMetadata) {
		DateTime now = new DateTime();
		if(rotatedIndexMetadata.getRebuiltIndexAlias() == null || (rotatedIndexMetadata.getDate() != null && rotatedIndexMetadata.getDate().isAfter(now.minusDays(LAG).toLocalDate()))) {
			// Only use rotated indexes for data that's ROTATION_LAG_DAYS old
			return rotatedIndexMetadata.getIndexNameAtBirth();
		}

		return rotatedIndexMetadata.getRebuiltIndexAlias();
	}

}
