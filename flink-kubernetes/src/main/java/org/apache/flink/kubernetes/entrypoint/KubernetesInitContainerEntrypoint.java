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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Entry point for the init Container to help download remote dependencies.
 */
public class KubernetesInitContainerEntrypoint {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesInitContainerEntrypoint.class);

	public static void main(String[] args) throws IOException {
		LOG.info("Starting init-container to download remote dependencies.");

		// startup checks and logging.
		EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesInitContainerEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		Preconditions.checkNotNull(
			configDir,
			"Flink configuration directory (%s) in environment should not be null!",
			ConfigConstants.ENV_FLINK_CONF_DIR);

		final Configuration configuration = GlobalConfiguration.loadConfiguration(configDir);

		final String jarsDownloadDir = configuration.get(KubernetesConfigOptions.JARS_DOWNLOAD_DIR);
		final String filesDownloadDir = configuration.get(KubernetesConfigOptions.FILES_DOWNLOAD_DIR);

		final List<String> remoteJarDependencies = configuration.get(KubernetesConfigOptionsInternal.REMOTE_JAR_DEPENDENCIES);
		final List<String> remoteFileDependencies = configuration.get(KubernetesConfigOptionsInternal.REMOTE_FILE_DEPENDENCIES);

		try {
			LOG.info("Start to download remote jars");
			fetchRemoteDependencies(remoteJarDependencies, jarsDownloadDir, configuration);
		} catch (Exception e) {
			LOG.error("Failed to download remote jars.", e);
			throw e;
		}

		try {
			LOG.info("Start to download remote files");
			fetchRemoteDependencies(remoteFileDependencies, filesDownloadDir, configuration);
		} catch (Exception e) {
			LOG.error("Failed to download remote files.", e);
			throw e;
		}

		LOG.info("Finished downloading application dependencies.");
	}

	private static void fetchRemoteDependencies(List<String> remoteDependencies,
			String targetDirectory,
			Configuration flinkConfig) throws IOException {
		// ------------------ Initialize the file systems -------------------------
		org.apache.flink.core.fs.FileSystem.initialize(
			flinkConfig,
			PluginUtils.createPluginManagerFromRootFolder(flinkConfig));

		// initialize file system
		// Copy the application master jar to the filesystem
		// Create a local resource to point to the destination jar path
		final HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
		final FileSystem fs = FileSystem.get(hdfsConfiguration);

		for (String remoteDependency: remoteDependencies) {
			final String localFileName = StringUtils.substringAfterLast(remoteDependency, "/");
			LOG.info("Copying from {} to {} ", remoteDependency, targetDirectory + "/" + localFileName);
			fs.copyToLocalFile(new Path(remoteDependency), new Path(targetDirectory + "/" + localFileName));
		}
	}
}

