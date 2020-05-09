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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in
 * {@link org.apache.flink.runtime.jobmanager.HighAvailabilityMode#FILESYSTEM}.
 *
 */
public class FileSystemCheckpointIDCounter implements CheckpointIDCounter {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemCheckpointIDCounter.class);

	/** The parent directory of the checkpoint-id-counter files. */
	private static final String HA_STORAGE_CHECKPOINT_ID_COUNTER = "checkpoint-id-counters";

	private AtomicLong checkpointIdCounter = new AtomicLong(1);

	/** The path for storing checkpointIDCounter. */
	private final Path checkpointIDCounterFilePath;

	private final FileSystem fs;

	private final Object startStopLock = new Object();

	@GuardedBy("startStopLock")
	private boolean isStarted;

	public FileSystemCheckpointIDCounter(JobID jobId, Path fileSystemHARootPath, FileSystem fs) {
		checkNotNull(jobId, "JobID");
		checkNotNull(fileSystemHARootPath, "FileSystemHARootPath");
		this.checkpointIDCounterFilePath = getCheckoutIDCounterFilePath(jobId, fileSystemHARootPath);
		this.fs = checkNotNull(fs);
	}

	@Override
	public void start() throws Exception {
		synchronized (startStopLock) {
			if (!isStarted) {

				if (fs.exists(checkpointIDCounterFilePath)) {
					LOG.info("Retrieving checkpoint id counter from {}.", checkpointIDCounterFilePath);
					checkpointIdCounter = InstantiationUtil.deserializeObject(
						fs.open(checkpointIDCounterFilePath),
						Thread.currentThread().getContextClassLoader());
					LOG.info("Retrieved checkpoint id {} from {}.", checkpointIdCounter.get(), checkpointIDCounterFilePath);
				}

				isStarted = true;
			}
		}

	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		synchronized (startStopLock) {
			if (isStarted) {
				LOG.info("Shutting down...");

				final Path checkpointIDCounterPath = checkpointIDCounterFilePath;
				if (jobStatus.isGloballyTerminalState()) {
					LOG.info("Removing checkpoint ID counter {} from FileSystem", checkpointIDCounterPath);
					if (fs.exists(checkpointIDCounterPath)) {
						fs.delete(checkpointIDCounterPath, true);
					}
				}

				isStarted = false;
			}
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		final long newCheckpointId = checkpointIdCounter.getAndIncrement();

		writeCheckpointIDCounterToFileSystem();

		return newCheckpointId;
	}

	@Override
	public long get() {
		return checkpointIdCounter.get();
	}

	@Override
	public void setCount(long newId) throws Exception {
		checkpointIdCounter.set(newId);

		writeCheckpointIDCounterToFileSystem();
	}

	private void writeCheckpointIDCounterToFileSystem() throws Exception {
		LOG.debug("Persistent current checkpointID counter {} into FileSystem.", checkpointIdCounter.get());

		FileSystemHAUtils.serializeStateToFileSystem(
			checkpointIdCounter,
			checkpointIDCounterFilePath,
			FileSystem.WriteMode.OVERWRITE,
			(exception) -> {
				throw new Exception("Could not open output stream for checkpoint id counter", exception);
			});
	}

	static Path getCheckoutIDCounterFilePath(@Nonnull JobID jobId, @Nonnull Path fileSystemHARootPath) {
		final Path checkpointIDCounterDirectory = new Path(fileSystemHARootPath, HA_STORAGE_CHECKPOINT_ID_COUNTER);
		return new Path(checkpointIDCounterDirectory, jobId.toHexString());
	}
}
