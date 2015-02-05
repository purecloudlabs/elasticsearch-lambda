package com.inin.analytics.elasticsearch.index.rotation;



public class ElasticsearchIndexRotationManagerNoop implements ElasticsearchIndexRotationManager {


	@Override
	public void registerIndexAvailableOnRotation(ElasticSearchIndexMetadata rotatedIndexMetadata) {
		
	}

	@Override
	public void updateRebuildPipelineState(RebuildPipelineState state) {
		
	}

	@Override
	public RebuildPipelineState getRebuildPipelineState() {
		return RebuildPipelineState.COMPLETE;
	}

	@Override
	public ElasticSearchIndexMetadata getElasticSearchIndexMetadata(String indexNameAtBirth) {
		ElasticSearchIndexMetadata rotatedIndexMetadata = new ElasticSearchIndexMetadata();
		rotatedIndexMetadata.setIndexNameAtBirth(indexNameAtBirth);
		return rotatedIndexMetadata;
	}

}
