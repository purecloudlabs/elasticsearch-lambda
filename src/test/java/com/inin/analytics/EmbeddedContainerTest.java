package com.inin.analytics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inin.analytics.elasticsearch.ESEmbededContainer;

public class EmbeddedContainerTest {
    private ESEmbededContainer container;
    private String snapshotWorkingLocation;
    private String snapshotRepoName;
    private String esWorkingDir;
    private int numShardsPerIndex;
    private String indexName;
    private static transient Logger logger = LoggerFactory.getLogger(EmbeddedContainerTest.class);

    @Before
    public void setup() {
        esWorkingDir = "/tmp/embeddedEStest/esrawdata1010/";
        snapshotWorkingLocation = "/tmp/embeddedEStest/bulkload110/";
        snapshotRepoName = "testbackup";
        indexName = "convtestindex";
        numShardsPerIndex = 1;
        
        String templateName = getTemplateName();
        String templateJson = getTemplate();

        ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
        .withNodeName("embededESTempLoaderNode")
        .withWorkingDir(esWorkingDir)
        .withClusterName("bulkLoadPartition")
        .withSnapshotWorkingLocation(snapshotWorkingLocation)
        .withCustomPlugin("customized_plugin_list")
        .withSnapshotRepoName(snapshotRepoName);
        
        if(templateName != null && templateJson != null) {
            builder.withTemplate(templateName, templateJson);   
        }
        
        if(container == null) {
            container = builder.build();   
        }

        //index setting
        org.elasticsearch.common.settings.Settings.Builder indexBuilder = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShardsPerIndex) 
                .put("index.refresh_interval", -1) 
                .put("index.number_of_replicas", 0)
                .put("index.translog.flush_threshold_size", "128mb")  
                .put("index.load_fixed_bitset_filters_eagerly", false)
                .put("index.merge.policy.max_merged_segment", "256mb")
                .put("index.merge.policy.max_merge_at_once", 10) 
                .put("index.merge.policy.segments_per_tier", 4)
                .put("index.merge.scheduler.max_thread_count", 1)
                .put("index.compound_format", false)
                .put("index.codec", "best_compression");
        Settings indexSettings = indexBuilder.build();

        //clear existing indices
        container.getNode().client().admin().indices().prepareDelete("*").get();

        GetIndexResponse indres = container.getNode().client().admin().indices().prepareGetIndex().get();
        assertEquals(indres.getIndices().length, 0);
        
        // Create index
        container.getNode().client().admin().indices().prepareCreate(indexName).setSettings(indexSettings).get();

        indres = container.getNode().client().admin().indices().prepareGetIndex().get();
        assertTrue(indres.getIndices()[0].equals(indexName));
    }

    private String getTemplateName() {
        return "test_template";
    }

    private String getTemplate() {
        ClassLoader classloader = this.getClass().getClassLoader();
        InputStream is = classloader.getResourceAsStream("elasticsearch-indicie-template-nested.json");
        String templateSource = null;
        try {
            templateSource = IOUtils.toString(is).replaceAll(" ", "").replaceAll("\t", "").replaceAll("\n", "");
        } catch (IOException e) {
            logger.error("Error in getting template.", e);
        }
        return templateSource;
    }

    @Test
    public void standardPluginTest() {
        AnalyzeResponse tokenRes = container.getNode().client().admin().indices().prepareAnalyze("this is a test").setAnalyzer("standard").get();
        assertEquals(tokenRes.getTokens().size(), 4);
    }

    @Test
    public void repositorySnapshotTest() {
        //create another snapshot
        PutRepositoryResponse response = container.getNode().client().admin().cluster().preparePutRepository("testrepo").setType("fs")
                .setSettings(Settings.builder()
                        .put("location", snapshotWorkingLocation)
                        .put("server_side_encryption", true))
                .get();
        assertTrue(response.isAcknowledged());
        
        //check both snapshots exist
        ClusterStateResponse clusterStateResponse = container.getNode().client().admin().cluster().prepareState().clear().setMetaData(true).get();
        MetaData metaData = clusterStateResponse.getState().getMetaData();
        RepositoriesMetaData repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
        assertNotNull(repositoriesMetaData.repository("testrepo"));
        assertNotNull(repositoriesMetaData.repository(snapshotRepoName));
    }

    @Test
    public void addIndexTest() {
        String testjson = "{\"customerId\":\"7821ad0f-a7a4-4321-9997-ccc9b89caabc\",\"color\":\"blue\",\"id\":\"00a23b34-0bc0-43b6-838e-9ff340ece3b1\",\"description\":\"MybwY6lUtmeyVbD\"}";
        IndexResponse response = container.getNode().client().prepareIndex("testindex", "testtype").setSource(testjson).execute().actionGet();
        assertEquals(response.status(), RestStatus.CREATED);
    }

    @Test
    public void snapshotRestoreTest() {
        //add data
        String indexType = "testtype";
        String docId = "7821ad0f-a7a4-4321-9997-ccc9b89caabc";
        String testjson  = "{\"customerId\":\"7821ad0f-a7a4-4321-9997-ccc9b89caabc\",\"color\":\"blue\",\"id\":\"00a23b34-0bc0-43b6-838e-9ff340ece3b1\",\"description\":\"MybwY6lUtmeyVbD\"}";
        String testjson2 = "{\"customerId\":\"5877065c-9305-4cbe-adc6-bae9766a620f\",\"color\":\"blue\",\"id\":\"483ad6dc-3cc5-4659-8d3b-99626933d469\",\"description\":\"IMpSc5T1uvk82U7\"}";
        String testjson3 = "{\"customerId\":\"7821ad0f-a7a4-4321-9997-ccc9b89caabc\",\"color\":\"yellow\",\"id\":\"b5f42e51-eb10-4c2d-aad1-19d7f9652415\",\"description\":\"YGAFGs73TZ4tLZl\"}";

        container.getNode().client().prepareIndex(indexName, indexType).setSource(testjson).setId(docId).execute().actionGet();
        container.getNode().client().prepareIndex(indexName, indexType).setSource(testjson2).setId(docId).execute().actionGet();
        container.getNode().client().prepareIndex(indexName, indexType).setSource(testjson3).setId(docId).execute().actionGet();
        GetResponse response = container.getNode().client().prepareGet(indexName, indexType, docId).get();
        assertTrue(response.isExists());

        //clear existing snapshots
        String snapshotName = "testsnapshot";
        List<SnapshotInfo> snapshots = container.getNode().client().admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        if (snapshots.size() > 0) {
            container.deleteSnapshot(snapshotName, snapshotRepoName);
            snapshots = container.getNode().client().admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        }
        assertEquals(snapshots.size(), 0);
        
        //create a snapshot
        container.snapshot(Arrays.asList(indexName), snapshotName, snapshotRepoName, null);
        snapshots = container.getNode().client().admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        assertEquals(snapshots.size(), 1);
        assertEquals(snapshots.get(0).snapshotId().getName(), snapshotName);
        
        //restore the snapshot
        try {
            //step1: remove existing data
            container.getNode().client().prepareDelete(indexName, indexType, docId).get();
            GetResponse getRes = container.getNode().client().prepareGet(indexName, indexType, docId).get();
            assertFalse(getRes.isExists());

            //step2: close indices
            CloseIndexResponse closeIndexRes = container.getNode().client().admin().indices().close(new CloseIndexRequest(indexName)).get();
            assertTrue(closeIndexRes.isAcknowledged());

            GetIndexResponse getIndexRes = container.getNode().client().admin().indices().prepareGetIndex().get();
            assertFalse(Arrays.asList(getIndexRes.getIndices()).contains(indexName));

            //step3: restore snapshot
            RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(snapshotRepoName, snapshotName).waitForCompletion(true);
            RestoreSnapshotResponse restoreSnapshotRes = container.getNode().client().admin().cluster().restoreSnapshot(restoreSnapshotRequest).actionGet();
            assertEquals(restoreSnapshotRes.status(), RestStatus.OK);

            //step4: confirm if data is restored
            getIndexRes = container.getNode().client().admin().indices().prepareGetIndex().get();
            assertTrue(Arrays.asList(getIndexRes.getIndices()).contains(indexName));
            assertTrue(container.getNode().client().prepareGet(indexName, indexType, docId).get().isExists());

        } catch (InterruptedException |ExecutionException e) {
            logger.error("Error in restoring snapshot.", e);
        }
    }

    @Test
    public void indexTemplateTest() {
        String templateSource = getTemplate();

        PutIndexTemplateResponse response = container.getNode().client().admin().indices().preparePutTemplate("emailtemplate").setSource(templateSource).get();
        assertTrue(response.isAcknowledged());

        GetIndexTemplatesResponse re = container.getNode().client().admin().indices().prepareGetTemplates("emailtemplate").get();
        assertEquals(re.getIndexTemplates().size(), 1);
    }

    @Test
    public void emailAnalyzerTest() {
        //email analyzer
        AnalyzeResponse emailRes = container.getNode().client().admin().indices().prepareAnalyze(indexName, "analyzer.email@inin.com").setAnalyzer("email").get();
        assertEquals(emailRes.getTokens().size(), 7);

        //field mapping
        emailRes = container.getNode().client().admin().indices().prepareAnalyze(indexName, "field.mapping@inin.com").setField("my_field").get();
        assertEquals(emailRes.getTokens().size(), 7);
    }

    @After
    public void shutdown() {
        try {
            container.getNode().close();
            while(!container.getNode().isClosed());
        } catch (IOException e) {
            logger.error("Error in closing embedded node", e);
        }
    }
}
