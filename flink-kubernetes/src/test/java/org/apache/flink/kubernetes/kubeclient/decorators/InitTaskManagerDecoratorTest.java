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

import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link InitJobManagerDecorator}.
 */
public class InitTaskManagerDecoratorTest extends TaskManagerDecoratorTest {

	private static final int RPC_PORT = 12345;

	private final Map<String, String> customizedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	private Pod resultPod;
	private Container resultMainContainer;

	@Before
	public void setup() throws IOException {
		flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(RPC_PORT));
		customizedEnvs.forEach((k, v) ->
			flinkConfig.setString(ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k, v));

		super.setup();

		final InitTaskManagerDecorator initTaskManagerDecorator =
			new InitTaskManagerDecorator(kubernetesTaskManagerParameters);

		final FlinkPod resultFlinkPod = initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		this.resultPod = resultFlinkPod.getPod();
		this.resultMainContainer = resultFlinkPod.getMainContainer();
	}

	@Test
	public void testMainContainerName() {
		Assert.assertEquals(
			kubernetesTaskManagerParameters.getTaskManagerMainContainerName(),
			this.resultMainContainer.getName());
	}

	@Test
	public void testMainContainerImage() {
		assertEquals(CONTAINER_IMAGE, this.resultMainContainer.getImage());
	}

	@Test
	public void testMainContainerImagePullPolicy() {
		assertEquals(CONTAINER_IMAGE_PULL_POLICY, this.resultMainContainer.getImagePullPolicy());
	}

	@Test
	public void testMainContainerResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(TASK_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(TOTAL_PROCESS_MEMORY + "Mi", requests.get("memory").getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(TASK_MANAGER_CPU), limits.get("cpu").getAmount());
		assertEquals(TOTAL_PROCESS_MEMORY + "Mi", limits.get("memory").getAmount());
	}

	@Test
	public void testMainContainerPorts() {
		final List<ContainerPort> expectedContainerPorts = Collections.singletonList(
			new ContainerPortBuilder()
				.withContainerPort(RPC_PORT)
			.build());

		assertThat(expectedContainerPorts, equalTo(this.resultMainContainer.getPorts()));
	}

	@Test
	public void testMainContainerEnv() {
		final Map<String, String> expectedEnvVars = new HashMap<>(customizedEnvs);
		expectedEnvVars.put(Constants.ENV_FLINK_POD_NAME, POD_NAME);

		final Map<String, String> resultEnvVars = this.resultMainContainer.getEnv()
			.stream()
			.collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));

		assertEquals(expectedEnvVars, resultEnvVars);
	}

	@Test
	public void testPodName() {
		assertEquals(POD_NAME, this.resultPod.getMetadata().getName());
	}

	@Test
	public void testPodLabels() {
		final Map<String, String> expectedLabels = new HashMap<String, String>() {
			{
				put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
				put(Constants.LABEL_APP_KEY, CLUSTER_ID);
				put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
			}
		};

		assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
	}
}
