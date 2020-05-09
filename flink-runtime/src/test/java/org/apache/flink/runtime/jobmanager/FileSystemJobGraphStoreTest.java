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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.TestLogger;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link FileSystemJobGraphStore}.
 */
public class FileSystemJobGraphStoreTest extends TestLogger {

	@Rule
	public final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private Path fileSystemHARootPath;
	private FileSystem fs;

	@Before
	public void before() throws IOException {
		fileSystemHARootPath = new Path(TEMPORARY_FOLDER.getRoot().toURI());
		fs = FileSystem.get(fileSystemHARootPath.toUri());
	}

	@Test
	public void testPutAndRemoveJobGraph() throws Exception {
		final FileSystemJobGraphStore jobGraphStore = createFileSystemJobGraphStore();

		try {
			jobGraphStore.start(null);

			// Empty state
			assertThat(jobGraphStore.getJobIds().size(), CoreMatchers.is(0));

			// Add initial
			JobGraph expectedJobGraph = createJobGraph(new JobID(), "JobName");
			jobGraphStore.putJobGraph(expectedJobGraph);

			Collection<JobID> jobIds = jobGraphStore.getJobIds();
			assertThat(jobIds.size(), CoreMatchers.is(1));

			JobID jobId = jobIds.iterator().next();
			verifyJobGraphs(expectedJobGraph, jobGraphStore.recoverJobGraph(jobId));

			// Update (same ID)
			expectedJobGraph = createJobGraph(expectedJobGraph.getJobID(), "Updated JobName");
			jobGraphStore.putJobGraph(expectedJobGraph);

			jobIds = jobGraphStore.getJobIds();
			assertThat(jobIds.size(), CoreMatchers.is(1));

			jobId = jobIds.iterator().next();
			verifyJobGraphs(expectedJobGraph, jobGraphStore.recoverJobGraph(jobId));

			// Remove
			jobGraphStore.removeJobGraph(expectedJobGraph.getJobID());
			assertThat(jobGraphStore.getJobIds().size(), CoreMatchers.is(0));

			// Don't fail if called again
			jobGraphStore.removeJobGraph(expectedJobGraph.getJobID());
		} finally {
			jobGraphStore.stop();
		}
	}

	@Test
	public void testRecoverJobGraphs() throws Exception {
		final FileSystemJobGraphStore jobGraphStore = createFileSystemJobGraphStore();

		try {
			jobGraphStore.start(null);

			final HashMap<JobID, JobGraph> expected = new HashMap<>();
			final JobID[] jobIds = new JobID[] { new JobID(), new JobID(), new JobID() };

			expected.put(jobIds[0], createJobGraph(jobIds[0], "Test JobGraph"));
			expected.put(jobIds[1], createJobGraph(jobIds[1], "Test JobGraph"));
			expected.put(jobIds[2], createJobGraph(jobIds[2], "Test JobGraph"));

			// Add all
			for (JobGraph jobGraph : expected.values()) {
				jobGraphStore.putJobGraph(jobGraph);
			}

			final Collection<JobID> actual = jobGraphStore.getJobIds();

			assertThat(actual.size(), CoreMatchers.is(expected.size()));

			for (JobID jobId : actual) {
				final JobGraph jobGraph = jobGraphStore.recoverJobGraph(jobId);
				assertThat(expected.containsKey(jobGraph.getJobID()), CoreMatchers.is(true));

				verifyJobGraphs(expected.get(jobGraph.getJobID()), jobGraph);

				jobGraphStore.removeJobGraph(jobGraph.getJobID());
			}

			// Empty state
			assertThat(jobGraphStore.getJobIds().size(), CoreMatchers.is(0));
		} finally {
			jobGraphStore.stop();
		}
	}

	@Nonnull
	private FileSystemJobGraphStore createFileSystemJobGraphStore() throws IOException {
		return new FileSystemJobGraphStore(fileSystemHARootPath, fs);
	}

	@Nonnull
	private JobGraph createJobGraph(JobID jobId, String jobName) {
		final JobGraph jobGraph = new JobGraph(jobId, jobName);

		final JobVertex jobVertex = new JobVertex("Test JobVertex");
		jobVertex.setParallelism(1);

		jobGraph.addVertex(jobVertex);

		return jobGraph;
	}

	private void verifyJobGraphs(JobGraph expected, JobGraph actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getJobID(), actual.getJobID());
	}
}
