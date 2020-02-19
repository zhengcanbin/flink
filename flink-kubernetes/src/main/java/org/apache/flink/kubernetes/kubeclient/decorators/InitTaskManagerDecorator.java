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

import org.apache.flink.kubernetes.kubeclient.parameter.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the TaskManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}.
 */
public class InitTaskManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

	public InitTaskManagerDecorator(KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
		super(kubernetesTaskManagerParameters.getFlinkConfiguration());
		this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
	}

	@Override
	protected Pod decoratePod(Pod pod) {
		return new PodBuilder(pod)
				.editOrNewMetadata()
					.withName(kubernetesTaskManagerParameters.getPodName())
					.withLabels(kubernetesTaskManagerParameters.getLabels())
					.endMetadata()
				.build();
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		final ResourceRequirements resourceRequirements = KubernetesUtils.getResourceRequirements(
				kubernetesTaskManagerParameters.getTaskManagerMemoryMB(),
				kubernetesTaskManagerParameters.getTaskManagerCPU());

		return new ContainerBuilder(container)
				.withName(kubernetesTaskManagerParameters.getTaskManagerMainContainerName())
				.withImage(kubernetesTaskManagerParameters.getImage())
				.withImagePullPolicy(kubernetesTaskManagerParameters.getImagePullPolicy())
				.withResources(resourceRequirements)
				.withPorts(new ContainerPortBuilder()
						.withContainerPort(kubernetesTaskManagerParameters.getRPCPort())
						.build())
				.withEnv(getCustomizedEnvs())
				.addNewEnv()
					.withName(ENV_FLINK_POD_NAME)
					.withValue(kubernetesTaskManagerParameters.getPodName())
					.endEnv()
				.build();
	}

	private List<EnvVar> getCustomizedEnvs() {
		return kubernetesTaskManagerParameters.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
			.collect(Collectors.toList());
	}
}
