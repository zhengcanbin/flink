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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.conf.AbstractKubernetesComponentConf;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 * Mounts the log4j.properties, logback.xml, and flink-conf.yaml configuration on the JobManager or TaskManager pod.
 */
public class FlinkConfMountDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesComponentConf kubernetesComponentConf;

	public FlinkConfMountDecorator(AbstractKubernetesComponentConf kubernetesComponentConf) {
		super(kubernetesComponentConf.getFlinkConfiguration());
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	protected Pod decoratePod(Pod pod) {
		final List<KeyToPath> keyToPaths = getLocalLogConfFiles().stream()
				.map(file -> new KeyToPathBuilder()
						.withKey(file.getName())
						.withPath(file.getName())
						.build())
				.collect(Collectors.toList());
		keyToPaths.add(new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());

		final Volume flinkConfVolume = new VolumeBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(getFlinkConfConfigMapName(kubernetesComponentConf.getClusterId()))
				.withItems(keyToPaths)
				.endConfigMap()
				.build();

		return new PodBuilder(pod)
				.editSpec()
				.addNewVolumeLike(flinkConfVolume)
				.endVolume()
				.endSpec()
				.build();
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		return new ContainerBuilder(container)
				.addNewVolumeMount()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(kubernetesComponentConf.getFlinkConfDirInPod())
				.endVolumeMount()
				.build();
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		final String clusterId = kubernetesComponentConf.getClusterId();

		final Map<String, String> data = new HashMap<>();

		final List<File> localLogFiles = getLocalLogConfFiles();
		for (File file : localLogFiles) {
			data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
		}
		data.put(FLINK_CONF_FILENAME, getFlinkConfData(this.configuration));

		final ConfigMap flinkConfConfigMap = new ConfigMapBuilder()
			.withNewMetadata()
				.withName(getFlinkConfConfigMapName(clusterId))
				.withLabels(kubernetesComponentConf.getCommonLabels())
				.endMetadata()
			.addToData(data)
			.build();

		return Collections.singletonList(flinkConfConfigMap);
	}

	@VisibleForTesting
	String getFlinkConfData(Configuration configuration) throws IOException {
		try (StringWriter sw = new StringWriter();
			PrintWriter out = new PrintWriter(sw)) {
			for (String key : configuration.keySet()) {
				String value = configuration.getString(key, null);
				out.print(key);
				out.print(": ");
				out.println(value);
			}

			return sw.toString();
		}
	}

	private List<File> getLocalLogConfFiles() {
		final String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
		final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);

		List<File> localLogFiles = new ArrayList<>();
		if (logbackFile.exists()) {
			localLogFiles.add(logbackFile);
		}
		if (log4jFile.exists()) {
			localLogFiles.add(log4jFile);
		}

		return localLogFiles;
	}

	@VisibleForTesting
	String getFlinkConfConfigMapName(String prefix) {
		return prefix + "-flink-conf";
	}
}
