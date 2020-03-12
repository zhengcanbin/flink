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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.entrypoint.KubernetesInitContainerEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Attach the jvm command and args to the init Container for running the
 * {@link org.apache.flink.kubernetes.entrypoint.KubernetesInitContainerEntrypoint}.
 */
public class JavaCmdInitContainerDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesParameters kubernetesParameters;

	public JavaCmdInitContainerDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		if (!kubernetesParameters.runInitContainer()) {
			return flinkPod;
		}

		final String startCommand = getInitContainerStartCommand(
			kubernetesParameters.getFlinkConfiguration(),
			kubernetesParameters.getFlinkConfDirInPod(),
			kubernetesParameters.getFlinkLogDirInPod(),
			kubernetesParameters.hasLogback(),
			kubernetesParameters.hasLog4j(),
			KubernetesInitContainerEntrypoint.class.getName());

		final Container initContainerWithStartCmd = new ContainerBuilder(flinkPod.getInitContainer())
			.withCommand(kubernetesParameters.getContainerEntrypoint())
			.withArgs(Arrays.asList("/bin/bash", "-c", startCommand))
			.build();

		return new FlinkPod.Builder(flinkPod)
			.withInitContainer(initContainerWithStartCmd)
			.build();
	}

	public static String getInitContainerStartCommand(
		Configuration flinkConfig,
		String configDirectory,
		String logDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		String mainClass) {
		return KubernetesUtils.getCommonStartCommand(
			flinkConfig,
			KubernetesUtils.ClusterComponent.INIT_CONTAINER,
			"",
			configDirectory,
			logDirectory,
			hasLogback,
			hasLog4j,
			mainClass,
			null);
	}
}
