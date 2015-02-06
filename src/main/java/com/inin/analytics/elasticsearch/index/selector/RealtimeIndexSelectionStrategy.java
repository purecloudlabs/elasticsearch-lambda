package com.inin.analytics.elasticsearch.index.selector;

import java.util.List;

import com.inin.analytics.elasticsearch.index.rotation.ElasticSearchIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;

public interface RealtimeIndexSelectionStrategy {
	ElasticsearchRoutingStrategy get(ElasticSearchIndexMetadata rotatedIndexMetadata);
	ElasticsearchRoutingStrategy getRoutingStrategyForIndicies(List<ElasticSearchIndexMetadata> indices);
	String getIndexWritable(ElasticSearchIndexMetadata rotatedIndexMetadata);
	String getIndexReadable(ElasticSearchIndexMetadata rotatedIndexMetadata);

}
