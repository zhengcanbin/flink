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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#FILESYSTEM}.
 *
 * <p>During recovery, the latest checkpoint is read from FileSystem. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 */
public class FileSystemCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemCompletedCheckpointStore.class);

	private static final Comparator<Tuple2<CompletedCheckpoint, String>> STRING_COMPARATOR = Comparator.comparing(o -> o.f1);

	/** The directory of the completed checkpoint files. */
	private static final String HA_STORAGE_COMPLETED_CHECKPOINT = "completed-checkpoints";

	/** The path for storing completed checkpoints. */
	private final Path completedCheckpointsPath;

	private final FileSystem fs;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/**
	 * Local copy of the completed checkpoints in FileSystem. This is restored from FileSystem
	 * when recovering and is maintained in parallel to the state in FileSystem during normal
	 * operations.
	 */
	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	private final Executor executor;

	public FileSystemCompletedCheckpointStore(
			JobID jobID,
			Path fileSystemHARootPath,
			FileSystem fs,
			int maxNumberOfCheckpointsToRetain,
			Executor executor) throws IOException {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		checkNotNull(fileSystemHARootPath, "FileSystemHARootPath");
		checkNotNull(jobID, "JobID");

		this.completedCheckpointsPath = getCompletedCheckpointsPath(fileSystemHARootPath, jobID);
		this.fs = fs;
		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
		this.executor = checkNotNull(executor);

		FileSystemHAUtils.createFileSystemPathIfRequired(completedCheckpointsPath, LOG);
	}

	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints in {} from FileSystem.", completedCheckpointsPath);

		final List<Tuple2<CompletedCheckpoint, String>> initialCheckpoints = new ArrayList<>();
		final FileStatus[] fileStats = fs.listStatus(completedCheckpointsPath);
		if (fileStats != null) {
			for (FileStatus fileStatus: fileStats) {
				final CompletedCheckpoint completedCheckpoint =
					InstantiationUtil.deserializeObject(fs.open(fileStatus.getPath()), Thread.currentThread().getContextClassLoader());
				initialCheckpoints.add(Tuple2.of(completedCheckpoint, fileStatus.getPath().getPath()));
			}
		}

		Collections.sort(initialCheckpoints, STRING_COMPARATOR);

		final int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in FileSystem.", numberOfInitialCheckpoints);

		completedCheckpoints.clear();
		for (Tuple2<CompletedCheckpoint, String> initialCheckpoint: initialCheckpoints) {
			completedCheckpoints.add(initialCheckpoint.f0);
		}
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Completed checkpoint");

		final String completedCheckpointFileName = FileSystemHAUtils.checkpointIdToPath(checkpoint.getCheckpointID());
		final Path completedCheckpointFilePath = new Path(completedCheckpointsPath, completedCheckpointFileName);

		// Now add the new one. If it fails, we don't want to loose existing data.
		FileSystemHAUtils.serializeStateToFileSystem(
			checkpoint,
			completedCheckpointFilePath,
			FileSystem.WriteMode.NO_OVERWRITE,
			(exception) -> {
				throw new Exception(
					String.format(
						"Writing completed checkpoint file %s to FileSystem failed",
						completedCheckpointFilePath),
					exception);
			});

		completedCheckpoints.addLast(checkpoint);

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			final CompletedCheckpoint completedCheckpoint = completedCheckpoints.removeFirst();
			tryRemoveCompletedCheckpoint(completedCheckpoint, CompletedCheckpoint::discardOnSubsume);
		}

		LOG.debug("Added {} to {}.", checkpoint, completedCheckpointFilePath);
	}

	private void tryRemoveCompletedCheckpoint(
			CompletedCheckpoint completedCheckpoint,
			ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {
		try {
			final String completedCheckpointFileName = FileSystemHAUtils.checkpointIdToPath(completedCheckpoint.getCheckpointID());
			final Path completedCheckpointFilePath = new Path(completedCheckpointsPath, completedCheckpointFileName);

			final boolean isSuccessRemoved = fs.delete(completedCheckpointFilePath, true);
			if (isSuccessRemoved) {
				executor.execute(() -> {
					try {
						discardCallback.accept(completedCheckpoint);
					}  catch (Exception e) {
						LOG.warn("Could not discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), e);
					}
				});
			}
		} catch (Exception e) {
			LOG.warn("Failed to subsume the old checkpoint", e);
		}
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (CompletedCheckpoint checkpoint: completedCheckpoints) {
				tryRemoveCompletedCheckpoint(
					checkpoint,
					completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus)
				);
			}

			completedCheckpoints.clear();

			LOG.info("Deleting completed checkpoint directory {} from FileSystem", completedCheckpointsPath);
			fs.delete(completedCheckpointsPath, true);
			LOG.info("Deleted completed checkpoint directory {} from FileSystem", completedCheckpointsPath);
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return new ArrayList<>(completedCheckpoints);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return false;
	}

	static Path getCompletedCheckpointsPath(Path fileSystemHARootPath, JobID jobID) {
		final Path completedCheckpointsDirectory = new Path(fileSystemHARootPath, HA_STORAGE_COMPLETED_CHECKPOINT);
		return new Path(completedCheckpointsDirectory, jobID.toHexString());
	}
}
