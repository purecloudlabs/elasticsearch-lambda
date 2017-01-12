package com.inin.analytics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.inin.analytics.elasticsearch.index.rotation.ElasticSearchIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;

public class ElasticsearchRoutingStrategyV1Test {

	private Set<String> orgIds = new HashSet<>();
	private Set<String> convIds = new HashSet<>();
	
	@Before
	public void setUp() throws Exception {
		// Note: this could be randomly generated, but if a test is to fail I want it to fail consistently 
		orgIds.add("ed1121bf-5e61-4ac5-ad99-c24f8c4f79db");
		orgIds.add("b8864a7e-98d9-4bef-af1e-54c8bea7ae40");
		orgIds.add("decccc4f-2c96-4f4c-890f-eb1433ff4c90");
		orgIds.add("1650943b-b125-41cf-9729-3bd3e164da16");
		orgIds.add("005a22cc-afbb-4bbe-97e9-6f1447293ed7");
		orgIds.add("e29469e1-02a1-4d63-91ef-40affca740a8");
		orgIds.add("400cdb2f-7573-444e-9612-e218ff1c8387");
		orgIds.add("aec66b84-6c34-466b-8991-031cba01241b");
		orgIds.add("53adf13e-ce20-4112-9809-6aa29c60dfa5");
		orgIds.add("f7f8ff19-81bf-49b1-a896-e996674d5a1f");
		orgIds.add("2eb8db9f-d3ae-4d9a-ac78-55cb792e0d2d");
		orgIds.add("3b984743-49bd-47d9-b38f-da3f822db03a");
		orgIds.add("b68edfd1-305f-4d31-9443-605ba05eb5cc");
		orgIds.add("0c8ce21d-3bb5-4dab-9531-1e2f3320259e");
		orgIds.add("254f6bec-8b3d-48d2-976a-ba4a3517558b");
		
		convIds.add("0a3fe8fa-0291-4a28-87c7-2eeeda2295cd");
		convIds.add("38b261be-23c4-4fe6-846c-f06231ddf82f");
		convIds.add("3e4602bb-9c72-4174-b29f-b72dee356716");
		convIds.add("3ff398ac-b832-4085-9ba3-0d2756c03f21");
		convIds.add("8773bd12-3fdc-452f-b440-60bee40fadfc");
		convIds.add("a0f20cbe-19a4-4aff-833d-c0919d6cfe73");
		convIds.add("de48d484-23ce-4e0d-b465-de91b2f6ad72");
		convIds.add("be57d96e-7ee8-4bba-bc35-15e347b69bed");
		convIds.add("7cb1b182-b64a-4815-bc61-87714dbd0431");
		convIds.add("8b9bbfbc-34dc-45f4-8dee-d82a44fc9995");
		convIds.add("60ecef71-0a30-4798-aae7-63f6c1df0cf0");
		convIds.add("64d0431b-bb68-4045-8fff-5ae2ed4eed51");
		convIds.add("2e8df74f-3536-4044-aa13-1c1b273ab29f");
	}
	
	@Test
	public void testOrgOn7ShardsHashes() {
		ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
		indexMetadata.setNumShards(10);
		indexMetadata.setNumShardsPerOrg(7);
		
		ElasticsearchRoutingStrategyV1 strategy = new ElasticsearchRoutingStrategyV1();
		strategy.configure(indexMetadata);
		
		
		for(String orgId : orgIds) {
			Set<String> routingHashs = new HashSet<>();
			for(String convId : convIds) {
				String routingHash = strategy.getRoutingHash(orgId, convId);
				routingHashs.add(routingHash);
			}
			// Data was spread across #numShardsPerOrg shards
			assertEquals(routingHashs.size(), 7);
			
			// Possible hashes contain the routing hashes
			String[] possibleHashes = strategy.getPossibleRoutingHashes(orgId);
			assertEquals(possibleHashes.length, 7);
			for(String possibleHash : possibleHashes) {
				assertTrue(routingHashs.contains(possibleHash));
			}
		}
	}
	
	@Test
	public void testOrgOn1ShardsHashes() {
		ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
		indexMetadata.setNumShards(5);
		indexMetadata.setNumShardsPerOrg(1);

		ElasticsearchRoutingStrategyV1 strategy = new ElasticsearchRoutingStrategyV1();
		strategy.configure(indexMetadata);
		for(String orgId : orgIds) {
			Set<String> routingHashs = new HashSet<>();
			for(String convId : convIds) {
				String routingHash = strategy.getRoutingHash(orgId, convId);
				routingHashs.add(routingHash);
			}
			// Data was spread across #numShardsPerOrg shards
			assertEquals(routingHashs.size(), 1);
			
			// Possible hashes contain the routing hashes
			String[] possibleHashes = strategy.getPossibleRoutingHashes(orgId);
			assertEquals(possibleHashes.length, 1);
			for(String possibleHash : possibleHashes) {
				assertTrue(routingHashs.contains(possibleHash));
			}
		}
	}
	
	@Test
	public void testSingleShardIndex() {
	    ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
	    indexMetadata.setNumShards(1);
	    indexMetadata.setNumShardsPerOrg(1);

	    ElasticsearchRoutingStrategyV1 strategy = new ElasticsearchRoutingStrategyV1();
	    strategy.configure(indexMetadata);
	    Set<String> routingHashs = new HashSet<>();
	    for(String orgId : orgIds) {
	        for(String convId : convIds) {
	            String routingHash = strategy.getRoutingHash(orgId, convId);
	            routingHashs.add(routingHash);
	        }
	    }
	    assertEquals(routingHashs.size(), 1);
	}

	@Test
	public void testOrgDistribution() {
		ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
		indexMetadata.setNumShards(5);
		indexMetadata.setNumShardsPerOrg(1);
		ElasticsearchRoutingStrategyV1 strategy = new ElasticsearchRoutingStrategyV1();
		strategy.configure(indexMetadata);
		
		Set<String> routingHashs = new HashSet<>();
		for(String orgId : orgIds) {
			routingHashs.add(strategy.getRoutingHash(orgId, "713729b0-91d1-4006-9317-8db4bc113be4"));
		}
		assertEquals(routingHashs.size(), indexMetadata.getNumShards());
	}

}
