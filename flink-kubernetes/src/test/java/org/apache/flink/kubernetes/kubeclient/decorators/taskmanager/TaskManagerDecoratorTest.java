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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.builder.FlinkPodBuilder;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesTaskManagerConf;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;

public class TaskManagerDecoratorTest {

	protected static final String CLUSTER_ID = "cluster-id-test";
	protected static final String CONTAINER_IMAGE = "flink:latest";
	protected static final String CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

	protected static final String POD_NAME = "taskmanager-pod-1";
	protected static final String DYNAMIC_PROPERTIES = "";

	protected static final int TOTAL_PROCESS_MEMORY = 1024;
	protected static final double TASK_MANAGER_CPU = 2.0;

	protected final Configuration flinkConfig = new Configuration();

	protected final TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils
		.newProcessSpecBuilder(flinkConfig)
		.withCpuCores(TASK_MANAGER_CPU)
		.withTotalProcessMemory(MemorySize.parse(TOTAL_PROCESS_MEMORY + "mb"))
		.build();

	protected final ContaineredTaskManagerParameters containeredTaskManagerParameters =
		new ContaineredTaskManagerParameters(taskExecutorProcessSpec, 4, new HashMap<>());

	protected final KubernetesTaskManagerConf kubernetesTaskManagerConf = new KubernetesTaskManagerConf(
		flinkConfig,
		POD_NAME,
		TOTAL_PROCESS_MEMORY,
		TASK_MANAGER_CPU,
		DYNAMIC_PROPERTIES,
		containeredTaskManagerParameters);

	protected final FlinkPod baseFlinkPod = new FlinkPodBuilder().build();

	@Before
	public void setup() throws IOException {
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, CONTAINER_IMAGE_PULL_POLICY);
	}
}
