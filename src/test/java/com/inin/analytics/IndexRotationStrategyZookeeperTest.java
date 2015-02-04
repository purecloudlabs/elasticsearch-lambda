package com.inin.analytics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.inin.analytics.elasticsearch.index.rotation.ExampleElasticsearchIndexRotationStrategyZookeeper;
import com.inin.analytics.elasticsearch.index.rotation.RebuildPipelineState;
import com.inin.analytics.elasticsearch.index.rotation.ESIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;
import com.inin.analytics.elasticsearch.index.selector.RealtimeIndexSelectionStrategyLagged;


@RunWith(MockitoJUnitRunner.class)
public class IndexRotationStrategyZookeeperTest {

	private static final Integer ZOOKEEPER_EMBEDED_PORT = 2181;
	private TestingServer zk = null;
	private ExampleElasticsearchIndexRotationStrategyZookeeper rotation;

	@Before
	public void setup() throws Exception {
		zk = new TestingServer(ZOOKEEPER_EMBEDED_PORT);
		rotation = new ExampleElasticsearchIndexRotationStrategyZookeeper();
		rotation.setCurator(getCurator());
		rotation.init();
	}

	public CuratorFramework getCurator() throws InterruptedException {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curator = CuratorFrameworkFactory.newClient("localhost", 10000, 10000, retryPolicy);
		curator.start();
		curator.getZookeeperClient().blockUntilConnectedOrTimedOut();
		return curator;
	}
	
	@After
	public void close() throws IOException {
		zk.close();
	}

	@Test
	public final void testRegisterAndGet() throws Exception {
		DateTime now = new DateTime();
					
		ESIndexMetadata metaData = new ESIndexMetadata();
		metaData.setIndexNameAtBirth("a");
		metaData.setNumShardsPerOrg(2);
		metaData.setRebuiltIndexAlias("b");
		metaData.setRebuiltIndexName("b");
		
		metaData.setRoutingStrategyClassName(ElasticsearchRoutingStrategyV1.class.getName());
		rotation.registerIndexAvailableOnRotation(metaData);

		// NodeCache curator recipe is async so we loop for up to 1sec waiting for the watcher to react. Polling sucks, but it beats Thread.sleep(1000) and generally happens in a few ms.
		long timer = System.currentTimeMillis();
		while(true) {
			ESIndexMetadata readMetaData = rotation.getRotatedIndexMetadata(metaData.getIndexNameAtBirth());
			assertEquals(readMetaData.getIndexNameAtBirth(), metaData.getIndexNameAtBirth());
			
			
			if(readMetaData.getRebuiltIndexName() != null) {
				// Assert that rotation lag is accounted for
				assertEquals(readMetaData.getRebuiltIndexName(), metaData.getRebuiltIndexName());

				break;
			}
			if(System.currentTimeMillis() - timer > 1000) {
				fail("NodeCache failed to update with latest value in a reasonable amount of time");
			}
		}
	}
	
	@Test
	public void testRealtimeIndexSelectionStrategyLagged() {
		DateTime now = new DateTime();
		ESIndexMetadata metaData = new ESIndexMetadata();
		metaData.setIndexNameAtBirth("a");
		metaData.setNumShardsPerOrg(2);
		metaData.setRebuiltIndexAlias("b");
		metaData.setRebuiltIndexName("bb");
		metaData.setRoutingStrategyClassName(ElasticsearchRoutingStrategyV1.class.getName());
		
		RealtimeIndexSelectionStrategyLagged strategy = new RealtimeIndexSelectionStrategyLagged(2);
		metaData.setDate(now.toLocalDate());
		assertEquals(strategy.getIndexReadable(metaData), "a");
		assertEquals(strategy.getIndexWritable(metaData), "a");

		metaData.setDate(now.minusDays(1).toLocalDate());
		assertEquals(strategy.getIndexReadable(metaData), "a");
		assertEquals(strategy.getIndexWritable(metaData), "a");

		metaData.setDate(now.minusDays(2).toLocalDate());
		assertEquals(strategy.getIndexReadable(metaData), "b");
		assertEquals(strategy.getIndexWritable(metaData), "bb");

		metaData.setDate(now.minusDays(3).toLocalDate());
		assertEquals(strategy.getIndexReadable(metaData), "b");
		assertEquals(strategy.getIndexWritable(metaData), "bb");
		
		metaData.setRebuiltIndexName(null);
		assertEquals(strategy.getIndexWritable(metaData), "a");
	}
	
	@Test
	public void testRebuildPipelineStateRunning() {
		rotation.updateRebuildPipelineState(RebuildPipelineState.RUNNING);
		assertEquals(rotation.getRebuildPipelineState(), RebuildPipelineState.RUNNING);
	}

	@Test
	public void testRebuildPipelineStateComplete() {
		rotation.updateRebuildPipelineState(RebuildPipelineState.COMPLETE);
		assertEquals(rotation.getRebuildPipelineState(), RebuildPipelineState.COMPLETE);
	}

}
