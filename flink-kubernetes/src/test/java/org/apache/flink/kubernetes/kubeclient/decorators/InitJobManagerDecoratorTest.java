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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameter.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link InitJobManagerDecorator}.
 */
public class InitJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

	private static final String SERVICE_ACCOUNT_NAME = "service-test";

	private Pod resultPod;
	private Container resultMainContainer;

	@Before
	public void setup() throws Exception {
		super.setup();
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);

		final InitJobManagerDecorator initJobManagerDecorator =
			new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod);

		this.resultPod = resultFlinkPod.getPod();
		this.resultMainContainer = resultFlinkPod.getMainContainer();
	}

	@Test
	public void testMainContainerName() {
		assertEquals(KubernetesJobManagerParameters.JOB_MANAGER_MAIN_CONTAINER_NAME, this.resultMainContainer.getName());
	}

	@Test
	public void testMainContainerImage() {
		assertEquals(CONTAINER_IMAGE, this.resultMainContainer.getImage());
		assertEquals(CONTAINER_IMAGE_PULL_POLICY, this.resultMainContainer.getImagePullPolicy());
	}

	@Test
	public void testMainContainerResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(JOB_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(JOB_MANAGER_MEMORY + "Mi", requests.get("memory").getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(JOB_MANAGER_CPU), limits.get("cpu").getAmount());
		assertEquals(JOB_MANAGER_MEMORY + "Mi", limits.get("memory").getAmount());
	}

	@Test
	public void testMainContainerPorts() {
		final List<ContainerPort> expectedContainerPorts = Arrays.asList(
			new ContainerPortBuilder()
				.withContainerPort(REST_PORT)
			.build(),
			new ContainerPortBuilder()
				.withContainerPort(RPC_PORT)
			.build(),
			new ContainerPortBuilder()
				.withContainerPort(BLOB_SERVER_PORT)
			.build());

		assertEquals(expectedContainerPorts, this.resultMainContainer.getPorts());
	}

	@Test
	public void testMainContainerEnv() {
		final List<EnvVar> envVars = this.resultMainContainer.getEnv();

		final Map<String, String> envs = new HashMap<>();
		envVars.forEach(env -> envs.put(env.getName(), env.getValue()));
		this.customizedEnvs.forEach((k, v) -> assertEquals(envs.get(k), v));

		assertTrue(envVars.stream().anyMatch(env -> env.getName().equals(ENV_FLINK_POD_IP_ADDRESS)
			&& env.getValueFrom().getFieldRef().getApiVersion().equals(API_VERSION)
			&& env.getValueFrom().getFieldRef().getFieldPath().equals(POD_IP_FIELD_PATH)));
	}

	@Test
	public void testPodLabels() {
		final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

		assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
	}

	@Test
	public void testPodServiceAccountName() {
		assertEquals(SERVICE_ACCOUNT_NAME, this.resultPod.getSpec().getServiceAccountName());
	}
}
