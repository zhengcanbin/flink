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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesTaskManagerConf;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.test.util.TestBaseUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link KubernetesTaskManagerBuilder}.
 */
public class KubernetesTaskManagerBuilderTest {

	private static final String _CLUSTER_ID = "cluster-id-test";
	private static final String _CONTAINER_IMAGE = "flink:latest";
	private static final String _CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

	private static final int _RPC_PORT = 12345;

	private static final String POD_NAME = "taskmanager-pod-1";
	private static final String DYNAMIC_PROPERTIES = "";

	private static final int TOTAL_PROCESS_MEMORY = 1024;
	private static final double TASK_MANAGER_CPU = 2.0;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Pod resultedPod;

	@Before
	public void setup() throws IOException {
		final File flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		final Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Configuration flinkConfig = new Configuration();
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, _CLUSTER_ID);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, _CONTAINER_IMAGE);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, _CONTAINER_IMAGE_PULL_POLICY);

		flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(_RPC_PORT));

		final TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils
			.newProcessSpecBuilder(flinkConfig)
			.withCpuCores(TASK_MANAGER_CPU)
			.withTotalProcessMemory(MemorySize.parse(TOTAL_PROCESS_MEMORY + "mb"))
			.build();

		final ContaineredTaskManagerParameters containeredTaskManagerParameters =
			new ContaineredTaskManagerParameters(taskExecutorProcessSpec, 4, new HashMap<>());

		final KubernetesTaskManagerConf kubernetesTaskManagerConf = new KubernetesTaskManagerConf(
			flinkConfig,
			POD_NAME,
			TOTAL_PROCESS_MEMORY,
			TASK_MANAGER_CPU,
			DYNAMIC_PROPERTIES,
			containeredTaskManagerParameters);

		this.resultedPod =
			KubernetesTaskManagerBuilder.buildTaskManagerComponent(kubernetesTaskManagerConf).getInternalResource();
	}

	@Test
	public void testPod() {
		assertEquals(POD_NAME, this.resultedPod.getMetadata().getName());
		assertEquals(3, this.resultedPod.getMetadata().getLabels().size());
		assertEquals(1, this.resultedPod.getSpec().getVolumes().size());
	}

	@Test
	public void testContainer() {
		final List<Container> resultedContainers = this.resultedPod.getSpec().getContainers();
		assertEquals(1, resultedContainers.size());

		final Container resultedMainContainer = resultedContainers.get(0);
		assertEquals(
			KubernetesTaskManagerConf.TASK_MANAGER_MAIN_CONTAINER_NAME,
			resultedMainContainer.getName());
		assertEquals(_CONTAINER_IMAGE, resultedMainContainer.getImage());
		assertEquals(_CONTAINER_IMAGE_PULL_POLICY, resultedMainContainer.getImagePullPolicy());
		// todo Resources
		assertEquals(1, resultedMainContainer.getPorts().size());
		assertEquals(1, resultedMainContainer.getCommand().size());
		assertEquals(3, resultedMainContainer.getArgs().size());
		assertEquals(1, resultedMainContainer.getVolumeMounts().size());
	}
}
