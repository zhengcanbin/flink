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
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
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

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 *
 */
public class FlinkConfConfigMapDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesMasterConf kubernetesMasterConf;

	public FlinkConfConfigMapDecorator(KubernetesMasterConf kubernetesMasterConf) {
		super(kubernetesMasterConf.getFlinkConfiguration());
		this.kubernetesMasterConf = kubernetesMasterConf;
	}

	@Override
	public FlinkPod configureFlinkPod(FlinkPod flinkPod) {
		final File localFlinkConfFile = getLocalFlinkConfFile();
		if (localFlinkConfFile == null) {
			return flinkPod;
		}

		final KeyToPath keyToPath = new KeyToPathBuilder()
			.withKey(localFlinkConfFile.getName())
			.withPath(localFlinkConfFile.getName())
			.build();

		final Volume flinkConfVolume = new VolumeBuilder()
			.withName(FLINK_CONF_VOLUME)
			.withNewConfigMap()
				.withName(getFlinkConfConfigMapName(kubernetesMasterConf.getClusterId()))
				.withItems(keyToPath)
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
				.withMountPath(kubernetesMasterConf.getInternalFlinkConfDir())
				.endVolumeMount()
			.build();

		return new FlinkPod(podWithFlinkConfVolume, containerWithFlinkConfVolumeMount);
	}

	@Override
	public List<HasMetadata> generateAdditionalKubernetesResources() throws IOException {
		final String clusterId = kubernetesMasterConf.getClusterId();

		final Map<String, String> flinkConfFileMap = new HashMap<>();
		final File localFlinkConfFile = getLocalFlinkConfFile();
		if (localFlinkConfFile == null) {
			return Collections.emptyList();
		}

		flinkConfFileMap.put(localFlinkConfFile.getName(), Files.toString(localFlinkConfFile, StandardCharsets.UTF_8));
		final ConfigMap flinkConfConfigMap = new ConfigMapBuilder()
			.withNewMetadata()
				.withName(getFlinkConfConfigMapName(clusterId))
				.withLabels(kubernetesMasterConf.getCommonLabels())
				.endMetadata()
			.addToData(flinkConfFileMap)
			.build();

		return Collections.singletonList(flinkConfConfigMap);
	}

	// todo change to optional type
	@Nullable
	private File getLocalFlinkConfFile() {
		final String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		final File flinkConfFile = new File(confDir, FLINK_CONF_FILENAME);

		if (flinkConfFile.exists()) {
			return flinkConfFile;
		}

		return null;
	}

	private String getFlinkConfConfigMapName(String prefix) {
		return prefix + "-flink-conf";
	}
}
