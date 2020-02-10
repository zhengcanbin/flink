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

package org.apache.flink.kubernetes.kubeclient.builder;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesMasterSpecification;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.common.MountVolumesDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.common.FlinkConfConfigMapDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.InitJobManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.RestServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.StartCommandMasterDecorator;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class KubernetesJobManagerBuilder {

	public static KubernetesMasterSpecification buildJobManagerComponent(
			KubernetesMasterConf kubernetesMasterConf) throws IOException {
		FlinkPod flinkPod = new FlinkPodBuilder().build();
		List<HasMetadata> additionalResources = new ArrayList<>();

		final List<KubernetesStepDecorator> stepDecorators = Arrays.asList(
			new InitJobManagerDecorator(kubernetesMasterConf),
			new StartCommandMasterDecorator(kubernetesMasterConf),
			new RestServiceDecorator(kubernetesMasterConf),
			new MountVolumesDecorator(kubernetesMasterConf),
			new FlinkConfConfigMapDecorator(kubernetesMasterConf));

		for (KubernetesStepDecorator stepDecorator: stepDecorators) {
			flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
			additionalResources.addAll(stepDecorator.buildAdditionalKubernetesResources());
		}

		final Deployment deployment = buildJobManagerDeployment(flinkPod, kubernetesMasterConf);

		return new KubernetesMasterSpecification(deployment, additionalResources);
	}

	private static Deployment buildJobManagerDeployment(
			FlinkPod flinkPod,
			KubernetesMasterConf kubernetesMasterConf) {
		final Container resolvedMainContainer = flinkPod.getMainContainer();

		final Pod resolvedPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToContainers(resolvedMainContainer)
				.endSpec()
			.build();

		final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

		return new DeploymentBuilder()
			.editOrNewMetadata()
				.withName(kubernetesMasterConf.getClusterId())
				.withLabels(kubernetesMasterConf.getCommonLabels())
				.withUid(kubernetesMasterConf.getClusterId())
				.endMetadata()
			.editOrNewSpec()
				.withReplicas(1)
				.editOrNewTemplate()
					.editOrNewMetadata()
						.withLabels(labels)	// todo 这里是否需要使用 Pod 的呢？？？
						.endMetadata()
					.withSpec(resolvedPod.getSpec())
					.endTemplate()
				.editOrNewSelector()
					.addToMatchLabels(labels)
					.endSelector()
				.endSpec()
			.build();
	}
}
