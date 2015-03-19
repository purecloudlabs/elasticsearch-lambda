package com.inin.analytics.elasticsearch.transport;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;
import com.inin.analytics.elasticsearch.BaseESReducer;

/**
 * Move the snapshot to locally connected storage
 * 
 * @author drew
 *
 */
public class LocalFSSnapshotTransport extends BaseTransport {

	public LocalFSSnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}

	@Override
	protected void init() {
		// no-op
	}

	@Override
	protected void close() {
		// no-op
	}

	@Override
	protected void transferFile(boolean deleteSource, String destination, String filename, String localDirectory) throws IOException {
		File source = new File(localDirectory + BaseESReducer.DIR_SEPARATOR + filename);
		Preconditions.checkArgument(source.exists(), "Could not find source file: " + source.getAbsolutePath()); 

		File destinationDir = new File(destination);
		FileUtils.forceMkdir(destinationDir);
		FileUtils.copyFileToDirectory(source, destinationDir);
		if(deleteSource) {
			source.delete();
		}
	}

	@Override
	protected void transferDir(String destination, String source, String shard) throws IOException {
		File sourceDir = new File(source);
		Preconditions.checkArgument(sourceDir.exists(), "Could not find dir: " + source); 
		
		File destinationDir = new File(destination + shard);
		FileUtils.forceMkdir(destinationDir);
		FileUtils.copyDirectory(sourceDir, destinationDir);
	}

	@Override
	protected boolean checkExists(String destination, Integer shardNumber) throws IOException {
		File destinationDir = new File(destination + shardNumber);
		return destinationDir.exists();
	}

}
