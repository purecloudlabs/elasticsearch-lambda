package com.inin.analytics.elasticsearch.example;

import org.apache.hadoop.mapred.JobConf;

import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.ConfigParams;
import com.inin.analytics.elasticsearch.ShardConfig;

public class ExampleIndexingReducerImpl extends BaseESReducer {
	
	/**
	 * Provide the JSON contents of the index template. This is your hook for configuring ElasticSearch.
	 * 
	 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-templates.html
	 */
	public String getTemplate() {
		return null;
	}

	/**
	 * Return a name for the index template. 
	 */
	@Override
	public String getTemplateName() {
		return null;
	}

    @Override
    public ShardConfig getShardConfig(JobConf job) {
        Long numShardsPerIndex = job.getLong(ConfigParams.NUM_SHARDS_PER_INDEX.name(), 5l);
        Long numShardsPerOrganization = job.getLong(ConfigParams.NUM_SHARDS_PER_ORGANIZATION.name(), 2l);
        return new ShardConfig(numShardsPerIndex, numShardsPerOrganization);
    }
    
}