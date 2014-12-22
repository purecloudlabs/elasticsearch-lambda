package com.inin.analytics.elasticsearch.index.routing;

import com.inin.analytics.elasticsearch.index.rotation.RotatedIndexMetadata;

public interface ElasticsearchRoutingStrategy extends java.io.Serializable {
	String getRoutingHash(String orgId, String convId);
	String[] getPossibleRoutingHashes(String orgId);
	void configure(RotatedIndexMetadata rotatedIndexMetadata);
	int getNumShardsPerOrg();
	int getNumShards();
}
