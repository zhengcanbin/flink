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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 */
public class KubernetesTaskManagerConf extends AbstractKubernetesComponentConf {

	private final Map<String, String> labels;

	private final String podName;

	private final int taskManagerMemoryMB;

	private final double taskManagerCPU;

	private final String dynamicProperties;

	private final ContaineredTaskManagerParameters containeredTaskManagerParameters;

	public KubernetesTaskManagerConf(
			Configuration flinkConfig,
			Map<String, String> labels,
			String podName,
			int taskManagerMemoryMB,
			double taskManagerCPU,
			String dynamicProperties,
			ContaineredTaskManagerParameters containeredTaskManagerParameters) {
		super(flinkConfig);
		this.labels = labels;
		this.podName = podName;
		this.taskManagerMemoryMB = taskManagerMemoryMB;
		this.taskManagerCPU = taskManagerCPU;
		this.dynamicProperties = dynamicProperties;
		this.containeredTaskManagerParameters = containeredTaskManagerParameters;
	}

	@Override
	public Map<String, String> getLabels() {
		return labels;
	}

	@Override
	public Map<String, String> getEnvironments() {
		return getPrefixedEnvironments(ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);
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
