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

package org.apache.flink.kubernetes.kubeclient.decorators.jobmanager;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.conf.AbstractKubernetesComponentConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;

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
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 *
 */
public class FlinkConfConfigMapDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesComponentConf kubernetesComponentConf;

	public FlinkConfConfigMapDecorator(AbstractKubernetesComponentConf kubernetesComponentConf) {
		super(kubernetesComponentConf.getFlinkConfiguration());
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod configureFlinkPod(FlinkPod flinkPod) {
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

		final Pod podWithFlinkConfVolume = new PodBuilder(flinkPod.getPod())
			.editSpec()
				.addNewVolumeLike(flinkConfVolume)
					.endVolume()
				.endSpec()
			.build();

		final Container containerWithFlinkConfVolumeMount = new ContainerBuilder(flinkPod.getMainContainer())
			.addNewVolumeMount()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(kubernetesComponentConf.getInternalFlinkConfDir())
				.endVolumeMount()
			.build();

		return new FlinkPod(podWithFlinkConfVolume, containerWithFlinkConfVolumeMount);
	}

	@Override
	public List<HasMetadata> generateAdditionalKubernetesResources() throws IOException {
		final String clusterId = kubernetesComponentConf.getClusterId();

		final Map<String, String> data = new HashMap<>();

		final List<File> localLogFiles = getLocalLogConfFiles();
		for (File file : localLogFiles) {
			data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
		}
		data.put(FLINK_CONF_FILENAME, getFlinkConfData());

		final ConfigMap flinkConfConfigMap = new ConfigMapBuilder()
			.withNewMetadata()
				.withName(getFlinkConfConfigMapName(clusterId))
				.withLabels(kubernetesComponentConf.getCommonLabels())
				.endMetadata()
			.addToData(data)
			.build();

		return Collections.singletonList(flinkConfConfigMap);
	}

	private String getFlinkConfData() throws IOException {
		final Properties properties = new Properties();
		properties.putAll(configuration.toMap());

		final StringWriter propertiesWriter = new StringWriter();
		properties.store(propertiesWriter, "Write flink configuration for configmap");

		return propertiesWriter.toString();
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

	private String getFlinkConfConfigMapName(String prefix) {
		return prefix + "-flink-conf";
	}
}
