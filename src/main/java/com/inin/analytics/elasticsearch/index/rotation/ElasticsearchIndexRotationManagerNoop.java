package com.inin.analytics.elasticsearch.index.rotation;



public class ElasticsearchIndexRotationManagerNoop implements ElasticsearchIndexRotationManager {


	@Override
	public void registerIndexAvailableOnRotation(ESIndexMetadata rotatedIndexMetadata) {
		
	}

	@Override
	public void updateRebuildPipelineState(RebuildPipelineState state) {
		
	}

	@Override
	public RebuildPipelineState getRebuildPipelineState() {
		return RebuildPipelineState.COMPLETE;
	}

	@Override
	public ESIndexMetadata getRotatedIndexMetadata(String indexNameAtBirth) {
		ESIndexMetadata rotatedIndexMetadata = new ESIndexMetadata();
		rotatedIndexMetadata.setIndexNameAtBirth(indexNameAtBirth);
		return rotatedIndexMetadata;
	}

}
