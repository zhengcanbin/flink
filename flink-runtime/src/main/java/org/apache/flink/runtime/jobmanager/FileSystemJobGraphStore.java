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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode#FILESYSTEM}.
 *
 */
public class FileSystemJobGraphStore implements JobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJobGraphStore.class);

	/** The directory of the job graph files. */
	private static final String HA_STORAGE_JOB_GRAPH = "job-graphs";

	/** Lock to synchronize with the {@link JobGraphListener}. */
	private final Object lock = new Object();

	/** The full configured path for all the job graphs. */
	private final Path jobGraphsPath;

	private final FileSystem fs;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	public FileSystemJobGraphStore(Path rootPath, FileSystem fs) throws IOException {
		this.jobGraphsPath = new Path(checkNotNull(rootPath), HA_STORAGE_JOB_GRAPH);
		this.fs = fs;

		FileSystemHAUtils.createFileSystemPathIfRequired(jobGraphsPath, LOG);
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		synchronized (lock) {
			if (!isRunning) {
				isRunning = true;
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (isRunning) {
				try {
					fs.delete(this.jobGraphsPath, true);
				} catch (Exception e) {
					throw new FlinkException("Could not properly stop the FileSystemJobGraphStore.", e);
				} finally {
					isRunning = false;
				}
			}
		}
	}

	@Nullable
	@Override
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");

		final String jobGraphFileName = getJobGraphFileNameFor(jobId);
		final Path jobGraphFilePath = new Path(jobGraphsPath, jobGraphFileName);

		LOG.debug("Recovering job graph {} from {}.", jobId, jobGraphFilePath);

		synchronized (lock) {
			verifyIsRunning();

			try {
				if (!fs.exists(jobGraphFilePath)) {
					return null;
				}
			} catch (Exception e) {
				throw new FlinkException("Could not retrieve the submitted job graph from " +
					jobGraphFilePath + " via the FileSystem job graph store.", e);
			}

			try (FSDataInputStream inStream = fs.open(jobGraphFilePath)) {
				final JobGraph recoveredJobGraph = InstantiationUtil.deserializeObject(inStream, Thread.currentThread().getContextClassLoader());
				LOG.info("Recovered {}.", recoveredJobGraph);
				return recoveredJobGraph;
			} catch (Exception e) {
				throw new FlinkException("Could not deserialize the stored job graph from " + jobGraphFilePath + " .", e);
			}
		}
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<JobID> jobIDs;

		LOG.debug("Retrieving all stored job ids from FileSystem under {}.", jobGraphsPath);

		final FileStatus[] fileStats = fs.listStatus(jobGraphsPath);
		if (fileStats == null) {
			jobIDs = Collections.emptyList();
		} else {
			jobIDs = new ArrayList<>(fileStats.length);
			for (FileStatus fileStat: fileStats) {
				final String jobGraphFileName = fileStat.getPath().getName();
				try {
					jobIDs.add(JobID.fromHexString(jobGraphFileName));
				} catch (Exception e) {
					LOG.warn("Could not parse job id from {}. This indicates a malformed path.", jobGraphFileName, e);
				}
			}
		}

		return jobIDs;
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");

		final Path jobGraphFilePath = new Path(jobGraphsPath, getJobGraphFileNameFor(jobGraph.getJobID()));

		LOG.debug("Adding job graph {} to {}.", jobGraph.getJobID(), jobGraphFilePath);

		synchronized (lock) {
			verifyIsRunning();

			FileSystemHAUtils.serializeStateToFileSystem(
				jobGraph,
				jobGraphFilePath,
				FileSystem.WriteMode.OVERWRITE,
				(exception) -> {
					throw new RuntimeException("Storing job graph " + jobGraph.getJobID() + " failed.", exception);
				});
		}

		LOG.info("Added job graph {} to {}.", jobGraph, jobGraphFilePath);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job graph");

		final Path jobGraphFilePath = new Path(jobGraphsPath, getJobGraphFileNameFor(jobId));

		LOG.info("Removing job graph {} from {}.", jobId, jobGraphFilePath);

		synchronized (lock) {
			try {
				fs.delete(jobGraphFilePath, true);
			} catch (Exception e) {
				LOG.warn("Removing job graph " + jobId + " failed.", e);
			}
		}

		LOG.debug("Removed job graph {} from {}.", jobId, jobGraphFilePath);
	}

	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		// ignore
	}

	/**
	 * Returns the JobID as a Hex String.
	 */
	private static String getJobGraphFileNameFor(JobID jobId) {
		return jobId.toHexString();
	}

	/**
	 * Verifies that the store is running.
	 */
	private void verifyIsRunning() {
		Preconditions.checkState(isRunning, "Not running. Forgot to call start()?");
	}
}
