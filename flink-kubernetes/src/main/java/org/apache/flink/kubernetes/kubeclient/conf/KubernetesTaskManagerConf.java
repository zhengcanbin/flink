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

package org.apache.flink.kubernetes.kubeclient.conf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesVolumeUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.util.Map;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_TASKMANAGER_VOLUMES_PREFIX;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters
 * that are used for constructing the TaskManager Pod.
 */
public class KubernetesTaskManagerConf extends AbstractKubernetesComponentConf {

	public static final String TASK_MANAGER_MAIN_CONTAINER_NAME = "flink-task-manager";

	private final String podName;

	private final int taskManagerMemoryMB;

	private final double taskManagerCPU;

	private final String dynamicProperties;

	private final ContaineredTaskManagerParameters containeredTaskManagerParameters;

	public KubernetesTaskManagerConf(
			Configuration flinkConfig,
			String podName,
			int taskManagerMemoryMB,
			double taskManagerCPU,
			String dynamicProperties,
			ContaineredTaskManagerParameters containeredTaskManagerParameters) {
		super(flinkConfig);
		this.podName = podName;
		this.taskManagerMemoryMB = taskManagerMemoryMB;
		this.taskManagerCPU = taskManagerCPU;
		this.dynamicProperties = dynamicProperties;
		this.containeredTaskManagerParameters = checkNotNull(containeredTaskManagerParameters);
	}

	@Override
	public Map<String, String> getLabels() {
		return KubernetesUtils.getTaskManagerLabels(getClusterId());
	}

	@Override
	public Map<String, String> getEnvironments() {
		return getPrefixedEnvironments(ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);
	}

	@Override
	public Tuple2<Volume[], VolumeMount[]> getVolumes() {
		return KubernetesVolumeUtils.constructVolumes(
			KubernetesVolumeUtils.parseVolumesWithPrefix(
				flinkConfig,
				KUBERNETES_TASKMANAGER_VOLUMES_PREFIX));
	}

	public String getTaskManagerMainContainerName() {
		return TASK_MANAGER_MAIN_CONTAINER_NAME;
	}

	public String getPodName() {
		return podName;
	}

	public int getTaskManagerMemoryMB() {
		return taskManagerMemoryMB;
	}

	public double getTaskManagerCPU() {
		return taskManagerCPU;
	}

	public int getRPCPort() {
		final int taskManagerRpcPort = KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
		checkArgument(taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());
		return taskManagerRpcPort;
	}

	public String getDynamicProperties() {
		return dynamicProperties;
	}

	public ContaineredTaskManagerParameters getContaineredTaskManagerParameters() {
		return containeredTaskManagerParameters;
	}
}
