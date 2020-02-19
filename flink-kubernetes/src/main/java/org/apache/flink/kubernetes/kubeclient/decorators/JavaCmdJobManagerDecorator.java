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
import org.apache.flink.kubernetes.kubeclient.parameter.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Creates the command and args for the main container which runs the JobManager code.
 */
public class JavaCmdJobManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

	public JavaCmdJobManagerDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		super(kubernetesJobManagerParameters.getFlinkConfiguration());
		this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		final String startCommand = getJobManagerStartCommand(
				configuration,
				kubernetesJobManagerParameters.getJobManagerMemoryMB(),
				kubernetesJobManagerParameters.getFlinkConfDirInPod(),
				kubernetesJobManagerParameters.getFlinkLogDirInPod(),
				kubernetesJobManagerParameters.hasLogback(),
				kubernetesJobManagerParameters.hasLog4j(),
				kubernetesJobManagerParameters.getEntrypointMainClass());

		return new ContainerBuilder(container)
				.withCommand(kubernetesJobManagerParameters.getContainerEntrypoint())
				.withArgs(Arrays.asList("/bin/bash", "-c", startCommand))
				.build();
	}

	/**
	 * Generates the shell command to start a jobmanager for kubernetes.
	 *
	 * @param flinkConfig The Flink configuration.
	 * @param jobManagerMemoryMb JobManager heap size.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @return A String containing the job manager startup command.
	 */
	private static String getJobManagerStartCommand(
			Configuration flinkConfig,
			int jobManagerMemoryMb,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			String mainClass) {
		final int heapSize = BootstrapTools.calculateHeapSize(jobManagerMemoryMb, flinkConfig);
		final String jvmMemOpts = String.format("-Xms%sm -Xmx%sm", heapSize, heapSize);
		return KubernetesUtils.getCommonStartCommand(
			flinkConfig,
			KubernetesUtils.ClusterComponent.JOB_MANAGER,
			jvmMemOpts,
			configDirectory,
			logDirectory,
			hasLogback,
			hasLog4j,
			mainClass,
			null);
	}
}
