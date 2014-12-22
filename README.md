# Elasticsearch-Lambda 

## Intro

A new way to bulk load elasticsearch from hadoop

 * Build indexes offline, without touching your production cluster
 * Run Elasticsearch unmodified, entirely within YARN
 * Build snapshots of indexes without requiring enough disk space on task trackers to hold an entire index
 

## How it works

The meat is in BaseEsReducer where individual reducer tasks recieve all the data for a single shard of a single index. It creates an embeded Elasticsearch instance, bulk loads it locally in-jvm, and then creates a snapshot. Discovery is disabled and the elasticsearch instances do not form a cluster with each other. Once bulk loading a shard is complete it is flushed, optimized, snapshotted, and then transfered to a snapshot repository (S3, HDFS, or Local FS). After the job is complete, any shards that have no data get placeholder shards generated to make the index complete.   

By making reducers only responsible for a single shard worth of data at a time, the total disk space required on task trackers is roughly

(shard data) + (shard snapshot) * (num reducers per task tracker)   

After indexes have been generated they can be loaded in using the snapshot restore functionality built into Elasticsearch. The index promotion process maintains state in Zookeeper. This is in the process of being open sourced.

## Can I use HDFS or NFS for Elasticsearch data?

Elasticsearch does not currently support backing it's data with HDFS, so this project makes use of local disks on the task trackers. Given that Solr Cloud already supports HDFS backed data, it's concievable that one day Elasticsearch might.

When considering NFS you must first consider how different hadoop distributions have implemented it. The apache version of hadoop implements NFS with large local disk buffers, so it may or may not save you any trouble. The Mapr NFS implementation is more native and performant. In our tests, running Elasticsearch on YARN and backing the data directories by NFS mounts backed by MapR-FS ran roughly half as fast. While impressive, it's up to you to balance the cost of using MapR for it's NFS cabilitiy to run Elasticsearch. Note, this requires substituting Elasticsearch's locking mechanism for a non-filesystem based implementation.

 
## Running Examples 
You can experiment via these run configs ran in series
 
 com.inin.analytics.elasticsearch.driver.Driver
 
Lets build some dummy data
 generateExampleData 1000 /tmp/data/part1
 
Prepare some data for the indexing job
 examplePrep /tmp/data/ /tmp/datajson/ _rebuild_20141030012508 5 2  

Build Elasticsearch indexes, snapshot them, and transport them to a snapshot repository on hdfs (s3 paths also allowed)
 esIndexRebuildExample /tmp/datajson/ /tmp/bulkload110/ hdfs:///tmp/snapshotrepo110/ my_backup /tmp/esrawdata1010/ 1 5 100 /tmp/manifest110/
