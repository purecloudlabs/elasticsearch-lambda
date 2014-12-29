package com.inin.analytics.elasticsearch.index.rotation;

import org.joda.time.LocalDate;

import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;

/**
 * Swapping rebuilt indexes into an ES cluster with zero downtime requires holding 
 * onto some state about the indexes. For example, if you rebuilt you cluster every night 
 * then depending on data retention you could have multiple date stamped [on creation] versions
 * of an index. Here we keep track of what version is in use. 
 * 
 * @author drew
 *
 */
public interface ElasticsearchIndexRotationStrategy {
	// Get the index that holds data for this date
	String getIndex(String indexNameAtBirth, LocalDate localDate);
	
	// This index has jumped no the rebuild train and can serve requests
	void registerIndexAvailableOnRotation(RotatedIndexMetadata rotatedIndexMetadata);
	
	// Get the routing strategy that was applied to the index when it was built 
	ElasticsearchRoutingStrategy getRoutingStrategy(String indexNameAtBirth, LocalDate indexDate);
	
	// Register that the pipeline is rebuilding indexes
	void updateRebuildPipelineState(RebuildPipelineState state);
	
	// Get the state of index rebuilding. This might be useful if you wish to defer writes during an index rebuild.
	RebuildPipelineState getRebuildPipelineState();
}
