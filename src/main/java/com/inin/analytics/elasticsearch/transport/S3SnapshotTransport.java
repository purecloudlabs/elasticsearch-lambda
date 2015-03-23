package com.inin.analytics.elasticsearch.transport;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Preconditions;
import com.inin.analytics.elasticsearch.BaseESReducer;

/**
 * Stitch together snapshots of ES shards as it pushes files to S3. If we could get the
 * snapshot gateway S3 plugin working we could potentially use that instead of using the aws sdk
 * directly, but there's some things holding that up. 
 * 
 * 1) That plugin needs some work to support MFA (at the time of writing it does not)
 * 2) We'll have to repackage ES with the plugin because installing plugins via the
 * embeded client isn't supported very well.
 * 3) This job is actually creating franken-snapshots. We're snapshotting each shard separately to reduce
 * the storage footprint per reducer task and then merging the snapshots together. To use the snapshot gateway
 * we would need a way to have the whole index on a single task tracker node. That means
 * beefy task trackers or mounting either some NFS or Ephemeral disks. Doing 1 shard at a time shrinks the problem.
 * 
 * @author drew
 *
 */
public class S3SnapshotTransport extends BaseTransport {
	private static transient Logger logger = LoggerFactory.getLogger(S3SnapshotTransport.class);
	private TransferManager tx;
	
	public S3SnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}
	
	@Override
	protected void init() {
		DefaultAWSCredentialsProviderChain credentialProviderChain = new DefaultAWSCredentialsProviderChain();
		tx = new TransferManager(credentialProviderChain.getCredentials());
	}
	
	@Override
	protected void close() {
		tx.shutdownNow();	
	}

	protected void transferDir(String shardDestinationBucket, String localShardPath, String shard) {
		MultipleFileUpload mfu = tx.uploadDirectory(shardDestinationBucket + shard, null, new File(localShardPath), true);
		/**
		 * TODO: Hadoop has a configurable timeout for how long a reducer can be non-responsive (usually 600s). If 
		 * this takes >600s hadoop will kill the task. We need to ping the reporter to let it know it's alive
		 * in the case where the file transfer is taking a while.
		 */
		while(!mfu.isDone()) {
			logger.info("Transfering to S3 completed %" + mfu.getProgress().getPercentTransferred());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	protected void transferFile(boolean deleteSource, String bucket, String filename, String localDirectory) {
		File source = new File(localDirectory + BaseESReducer.DIR_SEPARATOR + filename);
		Preconditions.checkArgument(source.exists(), "Could not find source file: " + source.getAbsolutePath()); 
		Upload upload = tx.upload(bucket,  filename, source);
		while(!upload.isDone());
		if(deleteSource) {
			source.delete();
		}
	}

	@Override
	protected boolean checkExists(String destination, Integer shard) throws IOException {
		// Break that s3 path into bucket & key 
		String[] pieces = StringUtils.split(destination, "/");
		String bucket = pieces[0];
		String key = destination.substring(bucket.length() + 1);
		
		// AWS SDK doesn't have an "exists" method so you have to list and check if the key is there. Thanks Obama
		ObjectListing objects = tx.getAmazonS3Client().listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(key));

		for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
			if (objectSummary.getKey().startsWith(key + shard)) {
				return true;
			}
		}
		return false;
	}

}
