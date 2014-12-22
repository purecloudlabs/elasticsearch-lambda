package com.inin.analytics.elasticsearch.transport;

public class SnapshotTransportStrategy {
	
	public enum STORAGE_SYSTEMS {
		s3,
		hdfs
	}

	/**
	 * Given a source & destination, return an appropriate transport implementation
	 * 
	 * @param snapshotWorkingLocation
	 * @param snapshotFinalDestination
	 * @return BaseTransport
	 */
	public static BaseTransport get(String snapshotWorkingLocation, String snapshotFinalDestination) {
		BaseTransport trasport = null;
		if(snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.s3.name())) {
			trasport = new S3SnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
		} else if(snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.hdfs.name())) {
				trasport = new HDFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
		} else {
			trasport = new LocalFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
		}
		return trasport;
	}
}
