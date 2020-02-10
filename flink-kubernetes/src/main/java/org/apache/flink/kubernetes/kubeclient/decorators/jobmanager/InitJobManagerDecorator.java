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

import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;

/**
 *
 */
public class InitJobManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesMasterConf kubernetesMasterConf;

	public InitJobManagerDecorator(KubernetesMasterConf kubernetesMasterConf) {
		super(kubernetesMasterConf.getFlinkConfiguration());
		this.kubernetesMasterConf = kubernetesMasterConf;
	}

	@Override
	protected Pod decoratePod(Pod pod) {
		return new PodBuilder(pod)
				.editOrNewMetadata()
				.withLabels(kubernetesMasterConf.getLabels())
				.endMetadata()
				.editOrNewSpec()
				// todo code base 没有设置 ServiceAccount
				.withServiceAccount(kubernetesMasterConf.getServiceAccount())
				.withServiceAccountName(kubernetesMasterConf.getServiceAccount())
				.endSpec()
				.build();
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		final ResourceRequirements requirements = KubernetesUtils.getResourceRequirements(
				kubernetesMasterConf.getJobManagerMemoryMB(),
				kubernetesMasterConf.getJobManagerCPU());

		return new ContainerBuilder(container)
				.withName(kubernetesMasterConf.getJobManagerMainContainerName())
				.withImage(kubernetesMasterConf.getImage())
				.withImagePullPolicy(kubernetesMasterConf.getImagePullPolicy())
				.withResources(requirements)
				.withPorts(buildContainerPortForContainer())
				.withEnv(buildEnvForContainer())
				.build();
	}

	// todo 缺少 Port 的名字啊
	private List<ContainerPort> buildContainerPortForContainer() {
		return Arrays.asList(
			new ContainerPortBuilder()
				.withContainerPort(kubernetesMasterConf.getRestPort())
				.build(),
			new ContainerPortBuilder()
				.withContainerPort(kubernetesMasterConf.getRPCPort())
				.build(),
			new ContainerPortBuilder().
				withContainerPort(kubernetesMasterConf.getBlobServerPort())
				.build());
	}

	private List<EnvVar> buildEnvForContainer() {
		final List<EnvVar> envList = kubernetesMasterConf.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
			.collect(Collectors.toList());

		envList.add(new EnvVarBuilder()
			.withName(ENV_FLINK_POD_IP_ADDRESS)
			.withValueFrom(new EnvVarSourceBuilder()
				.withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
				.build())
			.build());

		return envList;
	}

}
