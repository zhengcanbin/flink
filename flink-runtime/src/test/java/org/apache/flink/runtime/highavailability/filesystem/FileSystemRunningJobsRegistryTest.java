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
import org.apache.flink.util.TestLogger;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.junit.Assert.assertThat;

/**
 * Tests for basic {@link FileSystemRunningJobsRegistry}.
 */
public class FileSystemRunningJobsRegistryTest extends TestLogger {

	@ClassRule
	public final static TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private Path fileSystemHARootPath;
	private FileSystem fs;

	@Before
	public void before() throws IOException {
		fileSystemHARootPath = new Path(TEMPORARY_FOLDER.getRoot().toURI());
		fs = FileSystem.get(fileSystemHARootPath.toUri());
	}

	@Test
	public void testSetAndRetrieveJobSchedulingStatus() throws IOException {
		final FileSystemRunningJobsRegistry runningJobsRegistry = createFileSystemRunningJobsRegistry();

		final JobID jobID1 = new JobID();
		runningJobsRegistry.setJobRunning(jobID1);
		assertThat(
			runningJobsRegistry.getJobSchedulingStatus(jobID1),
			CoreMatchers.is(RunningJobsRegistry.JobSchedulingStatus.RUNNING));

		final JobID jobID2 = new JobID();
		runningJobsRegistry.setJobFinished(jobID2);
		assertThat(
			runningJobsRegistry.getJobSchedulingStatus(jobID2),
			CoreMatchers.is(RunningJobsRegistry.JobSchedulingStatus.DONE));

		runningJobsRegistry.clearJob(jobID1);
		assertThat(
			runningJobsRegistry.getJobSchedulingStatus(jobID1),
			CoreMatchers.is(RunningJobsRegistry.JobSchedulingStatus.PENDING));
		assertThat(
			runningJobsRegistry.getJobSchedulingStatus(jobID2),
			CoreMatchers.is(RunningJobsRegistry.JobSchedulingStatus.DONE));

		runningJobsRegistry.clearJob(jobID2);
		assertThat(
			runningJobsRegistry.getJobSchedulingStatus(jobID2),
			CoreMatchers.is(RunningJobsRegistry.JobSchedulingStatus.PENDING));
	}

	@Nonnull
	private FileSystemRunningJobsRegistry createFileSystemRunningJobsRegistry() throws IOException {
		return new FileSystemRunningJobsRegistry(fileSystemHARootPath, fs);
	}
}
