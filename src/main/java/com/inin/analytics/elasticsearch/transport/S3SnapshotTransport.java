package com.inin.analytics.elasticsearch.transport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
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
	private static final int S3_TRANSFER_THREAD_COUNT = 128;
	private TransferManager tx;
	private ObjectMetadataProvider objectMetadataProvider;

	/**
	 * The default S3 thread pool in the aws sdk is 10 threads. ES Snapshots can be 100s of files, so parallelizing that
	 * is advised. 
	 * 
	 * @return
	 */
	public static ThreadPoolExecutor createDefaultExecutorService() {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int threadCount = 1;

			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("s3-transfer-manager-worker-" + threadCount++);
				return thread;
			}
		};
		return (ThreadPoolExecutor)Executors.newFixedThreadPool(S3_TRANSFER_THREAD_COUNT, threadFactory);
	}

	public S3SnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}

	public static AmazonS3Client getS3Client() {
		return (Regions.getCurrentRegion() != null) ?
				Regions.getCurrentRegion().createClient(AmazonS3Client.class,
						new DefaultAWSCredentialsProviderChain(),
						new ClientConfiguration()) :
							new AmazonS3Client();
	}

	@Override
	protected void init() {
		tx = new TransferManager(getS3Client(), createDefaultExecutorService());
		
		objectMetadataProvider = new ObjectMetadataProvider() {
			@Override
			public void provideObjectMetadata(File file, ObjectMetadata metadata) {
				metadata.setSSEAlgorithm("AES256");
				metadata.setContentLength(file.length());
			}
		};
	}
	
	@Override
	protected void close() {
		tx.shutdownNow();	
	}

	protected void transferDir(String shardDestinationBucket, String localShardPath, String shard) {
		MultipleFileUpload mfu = tx.uploadDirectory(shardDestinationBucket + shard, null, new File(localShardPath), true, objectMetadataProvider);
		
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
		logger.info("Transfering + " + source + " to " + bucket + " with key " + filename);
		FileInputStream fis;
		try {
			fis = new FileInputStream(source);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setSSEAlgorithm("AES256");
			objectMetadata.setContentLength(source.length());
			Upload upload = tx.upload(bucket, filename, fis, objectMetadata);
			
			while(!upload.isDone());
			Preconditions.checkState(upload.getState().equals(TransferState.Completed), "File " + filename + " failed to upload with state: " + upload.getState());
			if(deleteSource) {
				source.delete();
			}
		} catch (FileNotFoundException e) {
			// Exception should never be thrown because the precondition above has already validated existence of file
			logger.error("Filename could not be found " + filename, e);
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

	@Override
    protected boolean checkExists(String destination, String filename) throws IOException {
        // Break that s3 path into bucket & key 
        String[] pieces = StringUtils.split(destination, "/");
        String bucket = pieces[0];
        String key = destination.substring(bucket.length() + 1);
        
        // AWS SDK doesn't have an "exists" method so you have to list and check if the key is there. Thanks Obama
        ObjectListing objects = tx.getAmazonS3Client().listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(key));

        for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
            if (objectSummary.getKey().startsWith(key + filename)) {
                return true;
            }
        }
        return false;
    }
}
