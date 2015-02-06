package com.inin.analytics.elasticsearch.index.rotation;


/**
 * Swapping rebuilt indexes into an ES cluster with zero downtime requires holding 
 * onto some state about the indexes. For example, if you rebuilt you cluster every night 
 * then depending on data retention you could have multiple date stamped [on creation] versions
 * of an index. Here we keep track of what version is in use. 
 * 
 * @author drew
 *
 */
public interface ElasticsearchIndexRotationManager {
	
	/**
	 * Look up meta data for an index based on the original name it was created with
	 * @param indexNameAtBirth
	 * @return
	 */
	ElasticSearchIndexMetadata getElasticSearchIndexMetadata(String indexNameAtBirth);
	
	/**
	 * Once a rebuilt index has been loaded into ElasticSearch, this registers the metadata
	 * with zookeeper so that other parts of the system know they can start using the new index. 
	 * @param rotatedIndexMetadata
	 */
	void registerIndexAvailableOnRotation(ElasticSearchIndexMetadata rotatedIndexMetadata);
	
	
	/**
	 * Optional: When rebuilding indexes, hold onto the state of the rebuild process. This is 
	 * useful if you wish to defer writes to an index being rebuilt until it's done and swapped in.  
	 *  
	 * @param state
	 */
	void updateRebuildPipelineState(RebuildPipelineState state);
	
	/**
	 * Get the current state of the index rebuild process. Again, this is optional and relies on updateRebuildPipelineState being used.
	 * 
	 * @return
	 */
	RebuildPipelineState getRebuildPipelineState();
}
