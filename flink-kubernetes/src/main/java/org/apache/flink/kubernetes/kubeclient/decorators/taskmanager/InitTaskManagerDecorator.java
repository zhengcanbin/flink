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

package org.apache.flink.kubernetes.kubeclient.decorators.taskmanager;

import org.apache.flink.kubernetes.kubeclient.conf.KubernetesTaskManagerConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NAME;

/**
 *
 */
public class InitTaskManagerDecorator extends AbstractKubernetesStepDecorator {
	private final KubernetesTaskManagerConf kubernetesTaskManagerConf;

	public InitTaskManagerDecorator(KubernetesTaskManagerConf kubernetesTaskManagerConf) {
		super(kubernetesTaskManagerConf.getFlinkConfiguration());
		this.kubernetesTaskManagerConf = kubernetesTaskManagerConf;
	}

	@Override
	protected Pod decoratePod(Pod pod) {
		return new PodBuilder(pod)
				.editOrNewMetadata()
				.withName(kubernetesTaskManagerConf.getPodName())
				.withLabels(kubernetesTaskManagerConf.getLabels())
				.endMetadata()
				.build();
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		final ResourceRequirements resourceRequirements = KubernetesUtils.getResourceRequirements(
				kubernetesTaskManagerConf.getTaskManagerMemoryMB(),
				kubernetesTaskManagerConf.getTaskManagerCPU());

		return new ContainerBuilder(container)
				.withName(kubernetesTaskManagerConf.getTaskManagerMainContainerName())
				.withImage(kubernetesTaskManagerConf.getImage())
				.withImagePullPolicy(kubernetesTaskManagerConf.getImagePullPolicy())
				.withResources(resourceRequirements)
				.withPorts(new ContainerPortBuilder()
						.withContainerPort(kubernetesTaskManagerConf.getRPCPort())
						.build())
				.withEnv(buildEnvForContainer())
				.build();
	}

	private List<EnvVar> buildEnvForContainer() {
		final List<EnvVar> envList = kubernetesTaskManagerConf.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
			.collect(Collectors.toList());

		envList.add(new EnvVarBuilder()
			.withName(ENV_FLINK_POD_NAME)
			.withValue(kubernetesTaskManagerConf.getPodName())
			.build());

		return envList;
	}
}
