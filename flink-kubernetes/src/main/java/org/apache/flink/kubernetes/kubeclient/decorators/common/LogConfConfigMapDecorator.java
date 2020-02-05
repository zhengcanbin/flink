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

package org.apache.flink.kubernetes.kubeclient.decorators.common;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.conf.AbstractKubernetesComponentConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.utils.Constants;

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.FLINK_LOG_CONF_VOLUME;

/**
 *
 */
public class LogConfConfigMapDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesComponentConf kubernetesComponentConf;

	public LogConfConfigMapDecorator(AbstractKubernetesComponentConf kubernetesComponentConf) {
		super(kubernetesComponentConf.getFlinkConfiguration());
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod configureFlinkPod(FlinkPod flinkPod) {
		final String clusterId = kubernetesComponentConf.getClusterId();

		final List<File> localLogConfFiles = getLocalLogConfFiles();

		if (localLogConfFiles.isEmpty()) {
			return flinkPod;
		}

		final List<KeyToPath> keyToPaths = localLogConfFiles.stream()
			.map(file -> new KeyToPathBuilder()
				.withKey(file.getName())
				.withPath(file.getName())
				.build())
			.collect(Collectors.toList());

		final Volume logConfVolume = new VolumeBuilder()
			.withName(FLINK_LOG_CONF_VOLUME)
			.withNewConfigMap()
				.withName(getLogConfigMapName(clusterId))
				.withItems(keyToPaths)
				.endConfigMap()
			.build();

		final Pod podWithLogConfVolume = new PodBuilder(flinkPod.getPod())
			.editSpec()
				.addNewVolumeLike(logConfVolume).endVolume()
				.endSpec()
			.build();

		final Container containerWithLogConfVolumeMount = new ContainerBuilder(flinkPod.getMainContainer())
			.addNewVolumeMount()
				.withName(FLINK_LOG_CONF_VOLUME)
				.withMountPath(kubernetesComponentConf.getInternalFlinkConfDir())
				.endVolumeMount()
			.build();

		return new FlinkPod(podWithLogConfVolume, containerWithLogConfVolumeMount);
	}

	@Override
	public List<HasMetadata> generateAdditionalKubernetesResources() throws IOException {
		final String clusterId = kubernetesComponentConf.getClusterId();

		final Map<String, String> logFileMap = new HashMap<>();

		final List<File> localLogFiles = getLocalLogConfFiles();
		for (File file : localLogFiles) {
			logFileMap.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
		}

		final ConfigMap logConfigMap = new ConfigMapBuilder()
			.withNewMetadata()
				.withName(getLogConfigMapName(clusterId))
				.withLabels(kubernetesComponentConf.getCommonLabels())
				.endMetadata()
			.addToData(logFileMap)
			.build();

		return Collections.singletonList(logConfigMap);
	}

	private List<File> getLocalLogConfFiles() {
		final String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		final File logbackFile = new File(confDir, Constants.CONFIG_FILE_LOGBACK_NAME);
		final File log4jFile = new File(confDir, Constants.CONFIG_FILE_LOG4J_NAME);

		List<File> localLogFiles = new ArrayList<>();
		if (logbackFile.exists()) {
			localLogFiles.add(logbackFile);
		}
		if (log4jFile.exists()) {
			localLogFiles.add(log4jFile);
		}

		return localLogFiles;
	}

	private String getLogConfigMapName(String prefix) {
		return prefix + "-log-conf";
	}

}
