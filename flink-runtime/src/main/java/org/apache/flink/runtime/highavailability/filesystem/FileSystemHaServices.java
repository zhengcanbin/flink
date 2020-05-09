/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.highavailability.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.FileSystemCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.FileSystemJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} using FileSystem.
 * The root path is configurable via the option {@link HighAvailabilityOptions#HA_FILESYSTEM_ROOT}.
 */
public class FileSystemHaServices implements HighAvailabilityServices {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemHaServices.class);

	/** The executor to run FileSystem callbacks on. */
	private final Executor executor;

	/** The fix address of the ResourceManager. */
	private final String resourceManagerAddress;

	/** The fix address of the Dispatcher. */
	private final String dispatcherAddress;

	/** The fix address of the cluster rest endpoint. */
	private final String clusterRestEndpointAddress;

	/** Store for arbitrary blobs. */
	private final BlobStoreService blobStoreService;

	/** Root Path for FileSystemHAServices. */
	private final Path fileSystemHARootPath;

	private final FileSystem fs;

	public FileSystemHaServices(
			String resourceManagerAddress,
			String dispatcherAddress,
			String clusterRestEndpointAddress,
			Path fileSystemHARootPath,
			Executor executor,
			BlobStoreService blobStoreService) throws IOException {
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress, "resourceManagerAddress");
		this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
		this.clusterRestEndpointAddress = checkNotNull(clusterRestEndpointAddress, clusterRestEndpointAddress);

		this.fileSystemHARootPath = checkNotNull(fileSystemHARootPath);
		this.executor = checkNotNull(executor);
		this.blobStoreService = checkNotNull(blobStoreService);

		this.fs = FileSystem.get(fileSystemHARootPath.toUri());

		FileSystemHAUtils.createFileSystemPathIfRequired(fileSystemHARootPath, LOG);
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return new StandaloneLeaderRetrievalService(resourceManagerAddress, DEFAULT_LEADER_ID);
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return new StandaloneLeaderRetrievalService(dispatcherAddress, DEFAULT_LEADER_ID);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return new StandaloneLeaderRetrievalService("UNKNOWN", DEFAULT_LEADER_ID);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return new StandaloneLeaderRetrievalService(defaultJobManagerAddress, DEFAULT_LEADER_ID);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return new StandaloneLeaderRetrievalService(clusterRestEndpointAddress, DEFAULT_LEADER_ID);
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return new StandaloneLeaderElectionService();
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return new StandaloneLeaderElectionService();
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return new StandaloneLeaderElectionService();
	}

	@Override
	public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
		return new StandaloneLeaderElectionService();
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new FileSystemCheckpointRecoveryFactory(fileSystemHARootPath, fs, executor);
	}

	@Override
	public JobGraphStore getJobGraphStore() throws Exception {
		return new FileSystemJobGraphStore(fileSystemHARootPath, fs);
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return new FileSystemRunningJobsRegistry(fileSystemHARootPath, fs);
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return blobStoreService;
	}

	@Override
	public void close() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.close();
		} catch (Throwable t) {
			exception = t;
		}

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close the FileSystemHaServices.");
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		LOG.info("Close and clean up all data for FileSystemHaServices.");

		Throwable exception = null;

		try {
			blobStoreService.closeAndCleanupAllData();
		} catch (Throwable t) {
			exception = t;
		}

		try {
			fs.delete(fileSystemHARootPath, true);
		} catch (Throwable t) {
			exception = t;
		}

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close and clean up all data of FileSystemHaServices.");
		}
	}
}
