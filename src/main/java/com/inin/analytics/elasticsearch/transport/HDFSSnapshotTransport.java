package com.inin.analytics.elasticsearch.transport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inin.analytics.elasticsearch.BaseESReducer;

public class HDFSSnapshotTransport  extends BaseTransport {
	private FileSystem hdfsFileSystem;
	private static transient Logger logger = LoggerFactory.getLogger(HDFSSnapshotTransport.class);
	
	public HDFSSnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}

	@Override
	protected void init() {
	    Configuration conf = new Configuration();
	    try {
			hdfsFileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to initialize HDFSSnapshotTransport because of ", e);
		}
	}

	@Override
	protected void close() {
		
	}
	
	private void ensurePathExists(String destination) throws IOException {
		String[] pieces = StringUtils.split(destination, BaseESReducer.DIR_SEPARATOR);
		
		String path = "";
		for(String piece : pieces) {
			if(StringUtils.isEmpty(piece)) {
				continue;
			}
			path = path + BaseESReducer.DIR_SEPARATOR + piece;
			if(!hdfsFileSystem.exists(new Path(path))) {
				try{
					hdfsFileSystem.mkdirs(new Path(path));	
				} catch (IOException e) {
					logger.warn("Unable to create path " + path + " likely because it was created in another reducer thread.");
				}
			}
		}
	}

	@Override
	protected void transferFile(String destination, String filename, String localDirectory) throws IOException {
		Path source = new Path(localDirectory + BaseESReducer.DIR_SEPARATOR + filename);
		ensurePathExists(destination);

		try{
			hdfsFileSystem.copyFromLocalFile(true, true, source, new Path(destination + BaseESReducer.DIR_SEPARATOR + filename));	
		}
		catch(LeaseExpiredException | RemoteException e) {
			// This is an expected race condition where 2 reducers are trying to write the manifest files at the same time. That's okay, it only has to succeed once. 
			logger.warn("Exception from 2 reducers writing the same file concurrently. One writer failed to obtain a lease. Destination " + destination + " filename " + filename + " localDirectory " + localDirectory, e);
		}
	}

	@Override
	protected void transferDir(String destination, String localShardPath, String shard) throws IOException {
		destination = destination + shard + BaseESReducer.DIR_SEPARATOR;
		ensurePathExists(destination);
		try{
			File[] files = new File(localShardPath).listFiles();
			for (File file : files) {
				transferFile(destination, file.getName(), localShardPath);
			}
		} catch(FileNotFoundException e) {
			throw new FileNotFoundException("Exception copying " + localShardPath + " to " + destination);
		} 
	}

	@Override
	protected boolean checkExists(String destination, Integer shardNumber) throws IOException {
		return hdfsFileSystem.exists(new Path(destination + shardNumber));
	}
}
