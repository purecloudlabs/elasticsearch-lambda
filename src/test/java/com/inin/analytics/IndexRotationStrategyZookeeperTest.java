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
import com.inin.analytics.elasticsearch.index.rotation.RotatedIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;


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
					
		RotatedIndexMetadata metaData = new RotatedIndexMetadata();
		metaData.setIndexNameAtBirth("a");
		metaData.setNumShardsPerOrg(2);
		metaData.setRebuiltIndexAlias("b");
		metaData.setRoutingStrategyClassName(ElasticsearchRoutingStrategyV1.class.getName());
		rotation.registerIndexAvailableOnRotation(metaData);

		// NodeCache curator recipe is async so we loop for up to 1sec waiting for the watcher to react. Polling sucks, but it beats Thread.sleep(1000) and generally happens in a few ms.
		long timer = System.currentTimeMillis();
		while(true) {
			assertEquals(rotation.getIndex("c", now.minusDays(10).toLocalDate()), "c");
			String index = rotation.getIndex("a", now.minusDays(10).toLocalDate());
			if(index.equals("b")) {
				// Assert that rotation lag is accounted for
				assertEquals(rotation.getIndex("a", now.toLocalDate()), "a");
				assertEquals(rotation.getIndex("a", now.minusDays(1).toLocalDate()), "a");
				assertEquals(rotation.getIndex("a", now.minusDays(2).toLocalDate()), "b");
				assertEquals(rotation.getIndex("a", now.minusDays(3).toLocalDate()), "b");

				break;
			}
			if(System.currentTimeMillis() - timer > 1000) {
				fail("NodeCache failed to update with latest value in a reasonable amount of time");
			}
		}
		ElasticsearchRoutingStrategy strategy = rotation.getRoutingStrategy(metaData.getIndexNameAtBirth(), now.minusDays(10).toLocalDate());
		assertEquals(strategy.getNumShardsPerOrg(), metaData.getNumShardsPerOrg());
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
