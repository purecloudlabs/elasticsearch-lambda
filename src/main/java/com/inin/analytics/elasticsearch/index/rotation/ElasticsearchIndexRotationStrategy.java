package com.inin.analytics.elasticsearch.index.rotation;

import org.joda.time.LocalDate;

import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;


public interface ElasticsearchIndexRotationStrategy {
	// Get the index that holds data for this date
	String getIndex(String indexNameAtBirth, LocalDate localDate);
	
	// This index has jumped no the rebuild train and can serve requests
	void registerIndexAvailableOnRotation(RotatedIndexMetadata rotatedIndexMetadata);
	
	// Get the routing strategy that was applied to the index when it was built 
	ElasticsearchRoutingStrategy getRoutingStrategy(String indexNameAtBirth, LocalDate indexDate);
	
	// Register that the pipeline is rebuilding indexes
	void updateRebuildPipelineState(RebuildPipelineState state);
	
	// Get the state of index rebuilding
	RebuildPipelineState getRebuildPipelineState();
}
