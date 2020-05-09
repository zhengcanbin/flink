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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

/**
 * Tests for basic {@link FileSystemCompletedCheckpointStore}.
 */
public class FileSystemCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

	@Rule
	public final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private JobID jobID;
	private Path fileSystemHARootPath;
	private FileSystem fs;

	@Before
	public void before() throws IOException {
		jobID = new JobID();
		fileSystemHARootPath = new Path(TEMPORARY_FOLDER.getRoot().toURI());
		fs = FileSystem.get(fileSystemHARootPath.toUri());
	}

	@Override
	protected CompletedCheckpointStore createCompletedCheckpoints(int maxNumberOfCheckpointsToRetain) throws Exception {
		return new FileSystemCompletedCheckpointStore(
			jobID,
			fileSystemHARootPath,
			fs,
			maxNumberOfCheckpointsToRetain,
			Executors.directExecutor());
	}

	/**
	 * Tests that older checkpoints are not cleaned up right away when recovering. Only after
	 * another checkpoint has been completed the old checkpoints exceeding the number of
	 * checkpoints to retain will be removed.
	 */
	@Test
	public void testRecover() throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(3);

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[]{
			createCheckpoint(0, sharedStateRegistry),
			createCheckpoint(1, sharedStateRegistry),
			createCheckpoint(2, sharedStateRegistry)
		};

		// Add multiple checkpoints
		checkpointStore.addCheckpoint(expected[0]);
		checkpointStore.addCheckpoint(expected[1]);
		checkpointStore.addCheckpoint(expected[2]);

		verifyCheckpointRegistered(expected[0].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[1].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[2].getOperatorStates().values(), sharedStateRegistry);

		// All three should be in FileSystem
		final Path completedCheckpointsPath =
			FileSystemCompletedCheckpointStore.getCompletedCheckpointsPath(fileSystemHARootPath, jobID);
		assertThat(fs.listStatus(completedCheckpointsPath).length, CoreMatchers.is(3));
		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(3));

		// Recover
		sharedStateRegistry.close();
		sharedStateRegistry = new SharedStateRegistry();
		checkpointStore.recover();

		assertThat(fs.listStatus(completedCheckpointsPath).length, CoreMatchers.is(3));
		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(3));
		assertThat(checkpointStore.getLatestCheckpoint(false), CoreMatchers.is(expected[2]));

		final List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
		expectedCheckpoints.add(expected[1]);
		expectedCheckpoints.add(expected[2]);
		expectedCheckpoints.add(createCheckpoint(3, sharedStateRegistry));

		checkpointStore.addCheckpoint(expectedCheckpoints.get(2));

		final List<CompletedCheckpoint> actualCheckpoints = checkpointStore.getAllCheckpoints();

		assertThat(actualCheckpoints, CoreMatchers.is(expectedCheckpoints));

		for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
			verifyCheckpointRegistered(actualCheckpoint.getOperatorStates().values(), sharedStateRegistry);
		}
	}

	@Test
	public void testShutdownDiscardsCheckpoints() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(1);
		final TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

		checkpointStore.addCheckpoint(checkpoint);

		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(1));
		assertThat(
			fs.listStatus(FileSystemCompletedCheckpointStore.getCompletedCheckpointsPath(fileSystemHARootPath, jobID)),
			CoreMatchers.is(CoreMatchers.notNullValue()));

		checkpointStore.shutdown(JobStatus.FINISHED);
		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(0));
		assertThat(
			fs.listStatus(FileSystemCompletedCheckpointStore.getCompletedCheckpointsPath(fileSystemHARootPath, jobID)),
			CoreMatchers.is(CoreMatchers.nullValue()));

		sharedStateRegistry.close();
		checkpointStore.recover();

		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(0));
	}

	/**
	 * Tests that suspends keeps all checkpoints (so that they can be recovered
	 * later by the FileSystem store).
	 */
	@Test
	public void testSuspendKeepsCheckpoints() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(1);
		final TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

		checkpointStore.addCheckpoint(checkpoint);

		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(1));
		assertThat(
			fs.listStatus(FileSystemCompletedCheckpointStore.getCompletedCheckpointsPath(fileSystemHARootPath, jobID)),
			CoreMatchers.is(CoreMatchers.notNullValue()));

		checkpointStore.shutdown(JobStatus.SUSPENDED);
		assertThat(checkpointStore.getNumberOfRetainedCheckpoints(), CoreMatchers.is(0));

		// Recover again
		sharedStateRegistry.close();
		checkpointStore.recover();

		final CompletedCheckpoint recovered = checkpointStore.getLatestCheckpoint(false);
		assertThat(recovered, CoreMatchers.is(checkpoint));
	}

	/**
	 * Tests that subsumed checkpoints are discarded.
	 */
	@Test
	public void testDiscardingSubsumedCheckpoints() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(1);

		final TestCompletedCheckpoint checkpoint1 = CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);
		checkpointStore.addCheckpoint(checkpoint1);
		assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

		final TestCompletedCheckpoint checkpoint2 = CompletedCheckpointStoreTest.createCheckpoint(1, sharedStateRegistry);
		checkpointStore.addCheckpoint(checkpoint2);
		final List<CompletedCheckpoint> allCheckpoints = checkpointStore.getAllCheckpoints();
		assertThat(allCheckpoints, Matchers.contains(checkpoint2));
		assertThat(allCheckpoints, Matchers.not(Matchers.contains(checkpoint1)));

		// verify that the subsumed checkpoint is discarded
		CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
	}

	/**
	 * Tests that the latest recovered checkpoint is the one with the highest checkpoint id
	 */
	@Test
	public void testLatestCheckpointRecovery() throws Exception {
		final int numCheckpoints = 3;

		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(numCheckpoints);
		final List<CompletedCheckpoint> checkpoints = new ArrayList<>(numCheckpoints);

		checkpoints.add(createCheckpoint(9, sharedStateRegistry));
		checkpoints.add(createCheckpoint(10, sharedStateRegistry));
		checkpoints.add(createCheckpoint(11, sharedStateRegistry));

		for (CompletedCheckpoint checkpoint : checkpoints) {
			checkpointStore.addCheckpoint(checkpoint);
		}

		sharedStateRegistry.close();
		checkpointStore.recover();

		final CompletedCheckpoint latestCheckpoint = checkpointStore.getLatestCheckpoint(false);
		assertThat(latestCheckpoint, CoreMatchers.is(checkpoints.get(checkpoints.size() -1)));
	}
}
