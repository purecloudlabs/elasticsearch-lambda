package com.inin.analytics.elasticsearch.index.rotation;

/**
 * When an index has been rebuilt and we're substituting it in, this class holds onto
 * some metadata about the rebuilt index.
 * 
 * @author drew
 *
 */
public class RotatedIndexMetadata {

	private String indexNameAtBirth;
	private String rebuiltIndexAlias;
	private int numShards;
	private int numShardsPerOrg;
	private String routingStrategyClassName;
	
	public String getIndexNameAtBirth() {
		return indexNameAtBirth;
	}
	public void setIndexNameAtBirth(String indexNameAtBirth) {
		this.indexNameAtBirth = indexNameAtBirth;
	}
	public String getRebuiltIndexAlias() {
		return rebuiltIndexAlias;
	}
	public void setRebuiltIndexAlias(String rebuiltIndexAlias) {
		this.rebuiltIndexAlias = rebuiltIndexAlias;
	}
	public int getNumShardsPerOrg() {
		return numShardsPerOrg;
	}
	public void setNumShardsPerOrg(int numShardsPerOrg) {
		this.numShardsPerOrg = numShardsPerOrg;
	}
	public String getRoutingStrategyClassName() {
		return routingStrategyClassName;
	}
	public void setRoutingStrategyClassName(String routingStrategyClassName) {
		this.routingStrategyClassName = routingStrategyClassName;
	}
	public int getNumShards() {
		return numShards;
	}
	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}
	
}
