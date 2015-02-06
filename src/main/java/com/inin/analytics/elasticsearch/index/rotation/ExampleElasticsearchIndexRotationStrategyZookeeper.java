package com.inin.analytics.elasticsearch.index.rotation;

public class ExampleElasticsearchIndexRotationStrategyZookeeper extends ElasticsearchIndexRotationManagerZookeeper  {
	private static final String INDEX_NAME_BASE_ZNODE = "/example/index/alias/";
	private static final String INDEX_REBUILD_PIPELINE_STATE_ZNODE = "/example/index/rebuild/pipeline/state";
	
	@Override
	protected String getBaseZnode() {
		return INDEX_NAME_BASE_ZNODE;
	}
	@Override
	protected String getRebuildStateZnode() {
		return INDEX_REBUILD_PIPELINE_STATE_ZNODE;
	}
}