package com.inin.analytics.elasticsearch.index.rotation;

import org.joda.time.LocalDate;

import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;


public class ElasticsearchIndexRotationStrategyNoop implements ElasticsearchIndexRotationStrategy {

	@Override
	public String getIndex(String indexNameAtBirth, LocalDate localDate) {
		return indexNameAtBirth;
	}

	@Override
	public void registerIndexAvailableOnRotation(RotatedIndexMetadata rotatedIndexMetadata) {
		
	}

	@Override
	public ElasticsearchRoutingStrategy getRoutingStrategy(String indexNameAtBirth, LocalDate indexDate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateRebuildPipelineState(RebuildPipelineState state) {
		
	}

	@Override
	public RebuildPipelineState getRebuildPipelineState() {
		return RebuildPipelineState.COMPLETE;
	}

}
