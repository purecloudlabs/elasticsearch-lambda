package com.inin.analytics.elasticsearch.index.selector;

import java.util.List;

import com.inin.analytics.elasticsearch.index.rotation.ESIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;

public interface RealtimeIndexSelectionStrategy {
	ElasticsearchRoutingStrategy get(ESIndexMetadata rotatedIndexMetadata);
	ElasticsearchRoutingStrategy getRoutingStrategyForIndicies(List<ESIndexMetadata> indices);
	String getIndexWritable(ESIndexMetadata rotatedIndexMetadata);
	String getIndexReadable(ESIndexMetadata rotatedIndexMetadata);

}
