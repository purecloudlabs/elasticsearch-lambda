package com.inin.analytics.elasticsearch.example;

import com.inin.analytics.elasticsearch.BaseESReducer;

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
}