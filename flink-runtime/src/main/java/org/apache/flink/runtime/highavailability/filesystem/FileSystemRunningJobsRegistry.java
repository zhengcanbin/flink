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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A FileSystem based registry for running jobs, high-availability depends on the FileSystem.
 */
public class FileSystemRunningJobsRegistry implements RunningJobsRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemRunningJobsRegistry.class);

	/** The directory of the running jobs registry. */
	private static final String HA_STORAGE_RUNNING_JOBS_REGISTRY = "running-jobs-registry";

	/** The full configured path for all the running jobs. */
	private final Path runningJobsRegistryPath;

	private final FileSystem fs;

	public FileSystemRunningJobsRegistry(Path fileSystemHARootPath, FileSystem fs) throws IOException {
		this.runningJobsRegistryPath = new Path(checkNotNull(fileSystemHARootPath), HA_STORAGE_RUNNING_JOBS_REGISTRY);
		this.fs = fs;

		FileSystemHAUtils.createFileSystemPathIfRequired(runningJobsRegistryPath, LOG);
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeJobSchedulingStatusToFileSystem(jobID, JobSchedulingStatus.RUNNING);
		} catch (Exception e) {
			throw new IOException("Failed to set RUNNING state in FileSystem for job " + jobID, e);
		}
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeJobSchedulingStatusToFileSystem(jobID, JobSchedulingStatus.DONE);
		} catch (Exception e) {
			throw new IOException("Failed to set DONE state in FileSystem for job " + jobID, e);
		}
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID);

		JobSchedulingStatus status = JobSchedulingStatus.PENDING;

		final Path runningJobRegistryPath = getRunningJobRegistryPathFor(jobID);
		if (fs.exists(runningJobRegistryPath)) {
			LOG.info("Retrieving job scheduling status for {} from {}.", jobID, runningJobRegistryPath);
			try {
				status = InstantiationUtil.deserializeObject(
					fs.open(runningJobRegistryPath),
					Thread.currentThread().getContextClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IOException(e);	// todo change the method signature to throw Exception
			}
		}

		return status;
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			final Path runningJobRegistryPath = getRunningJobRegistryPathFor(jobID);
			LOG.info("Removing {} from FileSystem", runningJobRegistryPath);
			fs.delete(runningJobRegistryPath, true);
			LOG.debug("Removed {} from FileSystem", runningJobRegistryPath);
		} catch (Exception e) {
			throw new IOException("Failed to clear job state from FileSystem for job " + jobID, e);
		}
	}

	private void writeJobSchedulingStatusToFileSystem(JobID jobID, JobSchedulingStatus status) throws Exception {
		LOG.debug("Setting scheduling state for job {} to {}.", jobID, status);
		// save to FileSystem
		FileSystemHAUtils.serializeStateToFileSystem(
			status,
			getRunningJobRegistryPathFor(jobID),
			FileSystem.WriteMode.OVERWRITE,
			(exception) -> {
				throw new Exception("Could not open output stream for job scheduling status", exception);
			});
	}

	private Path getRunningJobRegistryPathFor(JobID jobID) {
		return new Path(runningJobsRegistryPath, jobID.toString());
	}
}
