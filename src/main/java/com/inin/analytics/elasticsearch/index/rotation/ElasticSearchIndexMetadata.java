package com.inin.analytics.elasticsearch.index.rotation;

import org.joda.time.LocalDate;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * When an index has been rebuilt and we're substituting it in, this class holds onto
 * some metadata about the rebuilt index.
 * 
 * @author drew
 *
 */
public class ElasticSearchIndexMetadata {

	/**
	 * Name of the index when it was first created
	 */
	
	@Expose
	private String indexNameAtBirth;
	
	/**
	 * Name of the index after it was rebuilt
	 */
	
	@Expose
	private String rebuiltIndexName;

	/**
	 * A shortened alias for the rebuilt index
	 */
	
	@Expose
	private String rebuiltIndexAlias;

	/**
	 * Date associated with the index (if applicable such as date partitioned data)
	 */
	
	@SerializedName("indexLocalDate") 
	@Expose
	private LocalDate indexDate;
	
	/**
	 * Number if shards in the index
	 */
	
	@Expose
	private int numShards;
	
	/**
	 * Useful for the routing strategy, the number of shards an organization's data is split across within an index
	 */
	
	@Expose
	private int numShardsPerOrg;
	
	/**
	 * Class name for the routing strategy
	 */
	
	@Expose
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
	public String getRebuiltIndexName() {
		return rebuiltIndexName;
	}
	public void setRebuiltIndexName(String rebuiltIndexName) {
		this.rebuiltIndexName = rebuiltIndexName;
	}
	public LocalDate getDate() {
		return indexDate;
	}
	public void setDate(LocalDate date) {
		this.indexDate = date;
	}
	@Override
	public String toString() {
		return "RotatedIndexMetadata [indexNameAtBirth=" + indexNameAtBirth
				+ ", rebuiltIndexName=" + rebuiltIndexName
				+ ", rebuiltIndexAlias=" + rebuiltIndexAlias + ", indexDate="
				+ indexDate + ", numShards=" + numShards + ", numShardsPerOrg="
				+ numShardsPerOrg + ", routingStrategyClassName="
				+ routingStrategyClassName + "]";
	}
	
	
	
}
