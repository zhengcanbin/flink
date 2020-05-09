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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.util.FileSystemHAUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingConsumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link FileSystemHaServices}.
 */
public class FileSystemHaServicesTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final String resourceManagerAddress = "resourceManager";
	private final String dispatcherAddress = "dispatcher";
	private final String webMonitorAddress = "webMonitor";

	/**
	 * Tests that a simple {@link FileSystemHaServices#close()} does not delete FileSystem paths.
 	 */
	@Test
	public void testSimpleClose() throws Exception {
		final String rootPath = TEMPORARY_FOLDER.getRoot().toURI().toString() + "testSimpleClose";
		final Configuration configuration = createConfiguration(rootPath);

		final Path fileSystemHARootPath = FileSystemHAUtils.getFileSystemHANamespacedRootPath(configuration);
		final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

		runCleanupTest(fileSystemHARootPath, blobStoreService, FileSystemHaServices::close);

		assertThat(blobStoreService.isClosed(), is(true));
		assertThat(blobStoreService.isClosedAndCleanedUpAllData(), is(false));

		final FileStatus[] fileStats = FileSystem.get(fileSystemHARootPath.toUri()).listStatus(fileSystemHARootPath);
		assertThat(fileStats.length, is(not(0)));
	}

	@Test
	public void testSimpleCloseAndCleanupAllData() throws Exception {
		final String rootPath = TEMPORARY_FOLDER.getRoot().toURI().toString() + "testSimpleCloseAndCleanupAllData";
		final Configuration configuration = createConfiguration(rootPath);

		final Path fileSystemHARootPath = FileSystemHAUtils.getFileSystemHANamespacedRootPath(configuration);
		final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

		runCleanupTest(fileSystemHARootPath, blobStoreService, FileSystemHaServices::closeAndCleanupAllData);

		assertThat(blobStoreService.isClosedAndCleanedUpAllData(), is(true));

		final FileStatus[] fileStats = FileSystem.get(fileSystemHARootPath.toUri()).listStatus(fileSystemHARootPath);
		assertThat(fileStats, is(nullValue()));
	}

	private void runCleanupTest(
			Path fileSystemHARootPath,
			TestingBlobStoreService blobStoreService,
			ThrowingConsumer<FileSystemHaServices, Exception> fileSystemHaServicesConsumer) throws Exception {
		try (FileSystemHaServices fileSystemHaServices = new FileSystemHaServices(
			resourceManagerAddress,
			dispatcherAddress,
			webMonitorAddress,
			fileSystemHARootPath,
			Executors.directExecutor(),
			blobStoreService)) {

			final RunningJobsRegistry runningJobsRegistry = fileSystemHaServices.getRunningJobsRegistry();
			final JobID jobId = new JobID();
			runningJobsRegistry.setJobRunning(jobId);

			fileSystemHaServicesConsumer.accept(fileSystemHaServices);
		}
	}

	@Nonnull
	private Configuration createConfiguration(String rootPath) {
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_FILESYSTEM_ROOT, rootPath);
		return configuration;
	}

	private static class TestingBlobStoreService implements BlobStoreService {

		private boolean closedAndCleanedUpAllData = false;
		private boolean closed = false;

		@Override
		public void closeAndCleanupAllData() {
			closedAndCleanedUpAllData = true;
		}

		@Override
		public void close() throws IOException {
			closed = true;
		}

		@Override
		public boolean put(File localFile, JobID jobId, BlobKey blobKey) {
			return false;
		}

		@Override
		public boolean delete(JobID jobId, BlobKey blobKey) {
			return false;
		}

		@Override
		public boolean deleteAll(JobID jobId) {
			return false;
		}

		@Override
		public boolean get(JobID jobId, BlobKey blobKey, File localFile) {
			return false;
		}

		private boolean isClosed() {
			return closed;
		}

		private boolean isClosedAndCleanedUpAllData() {
			return closedAndCleanedUpAllData;
		}
	}
}
