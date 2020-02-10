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

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.utils.Constants;
import org.junit.Before;
import org.junit.Test;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.Pod;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class InitJobManagerDecoratorTest extends JobManagerDecoratorTest {

	private static final String _CONTAINER_IMAGE = "flink:latest";
	private static final String _CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

	private static final int _REST_PORT = 9081;
	private static final int _RPC_PORT = 7123;
	private static final int _BLOB_SERVER_PORT = 8346;

	private static final double _JOB_MANAGER_CPU = 2.0;
	private static final int _JOB_MANAGER_MEMORY = 768;

	private static final String _SERVICE_ACCOUNT_NAME = "service-test";

	private final Map<String, String> expectedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	private Pod resultPod;

	private Container resultMainContainer;

	@Before
	public void setup() throws IOException {
		super.setup();

		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, _CONTAINER_IMAGE);
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, _CONTAINER_IMAGE_PULL_POLICY);
		this.flinkConfig.set(RestOptions.PORT, _REST_PORT);
		this.flinkConfig.set(JobManagerOptions.PORT, _RPC_PORT);
		this.flinkConfig.set(BlobServerOptions.PORT, Integer.toString(_BLOB_SERVER_PORT));

		expectedEnvs.forEach((k, v) ->
			flinkConfig.setString(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + k, v));

		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, _JOB_MANAGER_CPU);
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, _SERVICE_ACCOUNT_NAME);

		final InitJobManagerDecorator initJobManagerDecorator =
			new InitJobManagerDecorator(this.kubernetesMasterConf);
		final FlinkPod resultFlinkPod = initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod);

		this.resultPod = resultFlinkPod.getPod();
		this.resultMainContainer = resultFlinkPod.getMainContainer();
	}

	@Test
	public void testMainContainerName() {
		assertEquals(KubernetesMasterConf.JOB_MANAGER_MAIN_CONTAINER_NAME, this.resultMainContainer.getName());
	}

	@Test
	public void testMainContainerImage() {
		assertEquals(_CONTAINER_IMAGE, this.resultMainContainer.getImage());
		assertEquals(_CONTAINER_IMAGE_PULL_POLICY, this.resultMainContainer.getImagePullPolicy());
	}

	@Test
	public void testMainContainerResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(_JOB_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(_JOB_MANAGER_MEMORY + "Mi", requests.get("memory").getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(_JOB_MANAGER_CPU), limits.get("cpu").getAmount());
		assertEquals(_JOB_MANAGER_MEMORY + "Mi", limits.get("memory").getAmount());
	}

	@Test
	public void testMainContainerPorts() {
		final List<ContainerPort> expectedContainerPorts = Arrays.asList(
			new ContainerPortBuilder()
				.withContainerPort(_REST_PORT)
			.build(),
			new ContainerPortBuilder()
				.withContainerPort(_RPC_PORT)
			.build(),
			new ContainerPortBuilder()
				.withContainerPort(_BLOB_SERVER_PORT)
			.build());

		assertThat(expectedContainerPorts, equalTo(this.resultMainContainer.getPorts()));
	}

	@Test
	public void testMainContainerEnv() {
		final List<EnvVar> envVars = this.resultMainContainer.getEnv();

		final Map<String, String> envs = new HashMap<>();
		envVars.forEach(env -> envs.put(env.getName(), env.getValue()));
		expectedEnvs.forEach((k, v) -> assertEquals(envs.get(k), v));

		assertTrue(envVars.stream().anyMatch(env -> env.getName().equals(ENV_FLINK_POD_IP_ADDRESS)
			&& env.getValueFrom().getFieldRef().getApiVersion().equals(API_VERSION)
			&& env.getValueFrom().getFieldRef().getFieldPath().equals(POD_IP_FIELD_PATH)));
	}

	@Test
	public void testPodLabels() {
		final Map<String, String> expectedLabels = new HashMap<String, String>() {
			{
				put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
				put(Constants.LABEL_APP_KEY, _CLUSTER_ID);
				put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
			}
		};

		assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
	}

	@Test
	public void testPodServiceAccountName() {
		assertEquals(_SERVICE_ACCOUNT_NAME, this.resultPod.getSpec().getServiceAccountName());
	}

}
