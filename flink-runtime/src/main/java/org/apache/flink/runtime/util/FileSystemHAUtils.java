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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.ThrowingConsumer;
import org.slf4j.Logger;

import java.io.IOException;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * Class containing helper functions for the FileSystem HA Services.
 */
public class FileSystemHAUtils {

	/**
	 * Gets the namespaced root path for the FileSystem HA Services.
	 *
	 * @param configuration The flink configuration
	 * @return Namespaced root path
	 */
	public static Path getFileSystemHANamespacedRootPath(Configuration configuration) {
		final String rootPath = configuration.getValue(HighAvailabilityOptions.HA_FILESYSTEM_ROOT);

		if (isNullOrWhitespaceOnly(rootPath)) {
			throw new IllegalConfigurationException("Configuration is missing the mandatory parameter: " +
				HighAvailabilityOptions.HA_FILESYSTEM_ROOT);
		}

		final Path path;
		try {
			path = new Path(rootPath);
		} catch (Exception e) {
			throw new IllegalConfigurationException("Invalid path for highly available storage (" +
				HighAvailabilityOptions.HA_FILESYSTEM_ROOT.key() + ')', e);
		}

		final Path rootPathWithNamespace;

		final String clusterId = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		try {
			rootPathWithNamespace = new Path(path, clusterId);
		} catch (Exception e) {
			throw new IllegalConfigurationException(
				String.format("Cannot create cluster high available storage path '%s/%s'. This indicates that an invalid cluster id (%s) has been specified.",
					rootPath,
					clusterId,
					HighAvailabilityOptions.HA_CLUSTER_ID.key()),
				e);
		}

		return rootPathWithNamespace;
	}

	/**
	 * Create a directory on FileSystem if it does not exist.
	 *
	 * @param path 		Path of the directory to be created.
	 * @param logger	Logger from callers.
	 * @throws IOException
	 */
	public static void createFileSystemPathIfRequired(Path path, Logger logger) throws IOException {
		final FileSystem fs = FileSystem.get(path.toUri());
		if (fs.exists(path)) {
			logger.warn("Found existing FileSystem storage directory at {}", path);
		} else {
			logger.info("Creating FileSystem storage directory at {}", path);
			fs.mkdirs(path);
			logger.debug("Created FileSystem storage directory at {}", path);
		}
	}

	/**
	 * Convert a checkpoint id into a FileSystem path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	public static String checkpointIdToPath(long checkpointId) {
		return String.format("%019d", checkpointId);
	}

	/**
	 * Stores the state in the given filesystem path.
	 *
	 * @param state     State to be serialized
	 * @param path		Given filesystem path that save the state
	 * @param writeMode	Whether or not overwrite existing file
	 * @param consumer	Latest exception consumer
	 * @throws Exception
	 */
	public static <S> void serializeStateToFileSystem(
			S state,
			Path path,
			FileSystem.WriteMode writeMode,
			ThrowingConsumer<Exception, Exception> consumer) throws Exception {
		Exception latestException = null;
		for (int attempt = 0; attempt < 10; attempt++) {
			try {
				final FileSystem fs = FileSystem.get(path.toUri());
				FSDataOutputStream outStream = fs.create(path, writeMode);
				InstantiationUtil.serializeObject(outStream, state);
				latestException = null;
				break;
			} catch (Exception e) {
				latestException = e;
			}
		}
		if (latestException != null) {
			consumer.accept(latestException);
		}
	}
}
