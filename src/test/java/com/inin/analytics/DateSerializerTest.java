package com.inin.analytics;

import static org.junit.Assert.*;

import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.gson.Gson;
import com.inin.analytics.elasticsearch.index.rotation.ElasticSearchIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;
import com.inin.analytics.elasticsearch.util.GsonFactory;

public class DateSerializerTest {

	@Test
	public void test() {
		ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
		indexMetadata.setDate(new LocalDate());
		indexMetadata.setIndexNameAtBirth("test");
		indexMetadata.setNumShards(2);
		indexMetadata.setNumShardsPerOrg(3);
		indexMetadata.setRebuiltIndexAlias("alias");
		indexMetadata.setRoutingStrategyClassName(ElasticsearchRoutingStrategyV1.class.getName());
		Gson gson = GsonFactory.buildGsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
		String json = gson.toJson(indexMetadata);
		
		assertEquals(json, "{\"indexNameAtBirth\":\"test\",\"rebuiltIndexAlias\":\"alias\",\"indexLocalDate\":\"2015-02-09\",\"numShards\":2,\"numShardsPerOrg\":3,\"routingStrategyClassName\":\"com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1\"}");
		ElasticSearchIndexMetadata indexMetadataDeserialized = gson.fromJson(json, ElasticSearchIndexMetadata.class);
		assertEquals(indexMetadata.getDate(), indexMetadataDeserialized.getDate());
		assertEquals(indexMetadata.getIndexNameAtBirth(), indexMetadataDeserialized.getIndexNameAtBirth());
		assertEquals(indexMetadata.getNumShards(), indexMetadataDeserialized.getNumShards());
		assertEquals(indexMetadata.getNumShardsPerOrg(), indexMetadataDeserialized.getNumShardsPerOrg());
		assertEquals(indexMetadata.getRebuiltIndexAlias(), indexMetadataDeserialized.getRebuiltIndexAlias());
		assertEquals(indexMetadata.getRebuiltIndexName(), indexMetadataDeserialized.getRebuiltIndexName());
		assertEquals(indexMetadata.getRoutingStrategyClassName(), indexMetadataDeserialized.getRoutingStrategyClassName());
	}
	
	@Test
	public void testBackwardsCompatibility() {
		// We had a case where the indexDate was serialized wrong. This has since been fixed, but this test verifies that the indexDate is ignored and left null rather than blowing up on deserialize
		String oldJson = "\n" + 
				"\n" + 
				"{\n" + 
				"  \"indexNameAtBirth\": \"c141031\",\n" + 
				"  \"rebuiltIndexName\": \"c141031_build_399_20150206012509\",\n" + 
				"  \"rebuiltIndexAlias\": \"c141031r\",\n" + 
				"  \"indexDate\": {\n" + 
				"    \"iLocalMillis\": 1414713600000,\n" + 
				"    \"iChronology\": {\n" + 
				"      \"iBase\": {\n" + 
				"        \"iMinDaysInFirstWeek\": 4\n" + 
				"      }\n" + 
				"    }\n" + 
				"  },\n" + 
				"  \"numShards\": 5,\n" + 
				"  \"numShardsPerOrg\": 2,\n" + 
				"  \"routingStrategyClassName\": \"com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1\"\n" + 
				"}\n" + 
				"\n" + 
				"";
		Gson gson = GsonFactory.buildGsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
		ElasticSearchIndexMetadata indexMetadataDeserialized = gson.fromJson(oldJson, ElasticSearchIndexMetadata.class);
		assertNull(indexMetadataDeserialized.getDate());
		assertEquals("c141031", indexMetadataDeserialized.getIndexNameAtBirth());
		assertEquals(5, indexMetadataDeserialized.getNumShards());
		assertEquals(2, indexMetadataDeserialized.getNumShardsPerOrg());
		assertEquals("c141031r", indexMetadataDeserialized.getRebuiltIndexAlias());
		assertEquals("c141031_build_399_20150206012509", indexMetadataDeserialized.getRebuiltIndexName());
		assertEquals("com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1", indexMetadataDeserialized.getRoutingStrategyClassName());
	}
	
	@Test
	public void testMissingDate() {
		// We had a case where the indexDate was serialized wrong. This has since been fixed, but this test verifies that the indexDate is ignored and left null rather than blowing up on deserialize
		String oldJson = "\n" + 
				"\n" + 
				"{\n" + 
				"  \"indexNameAtBirth\": \"c141031\",\n" + 
				"  \"rebuiltIndexName\": \"c141031_build_399_20150206012509\",\n" + 
				"  \"rebuiltIndexAlias\": \"c141031r\",\n" + 
				"  \"numShards\": 5,\n" + 
				"  \"numShardsPerOrg\": 2,\n" + 
				"  \"routingStrategyClassName\": \"com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1\"\n" + 
				"}\n" + 
				"\n" + 
				"";
		Gson gson = GsonFactory.buildGsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
		ElasticSearchIndexMetadata indexMetadataDeserialized = gson.fromJson(oldJson, ElasticSearchIndexMetadata.class);
		assertNull(indexMetadataDeserialized.getDate());
		assertEquals("c141031", indexMetadataDeserialized.getIndexNameAtBirth());
		assertEquals(5, indexMetadataDeserialized.getNumShards());
		assertEquals(2, indexMetadataDeserialized.getNumShardsPerOrg());
		assertEquals("c141031r", indexMetadataDeserialized.getRebuiltIndexAlias());
		assertEquals("c141031_build_399_20150206012509", indexMetadataDeserialized.getRebuiltIndexName());
		assertEquals("com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1", indexMetadataDeserialized.getRoutingStrategyClassName());
	}
}
