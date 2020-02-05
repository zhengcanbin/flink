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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.builder.FlinkPodBuilder;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 *
 */
public class StartCommandMasterDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesMasterConf kubernetesMasterConf;

	public StartCommandMasterDecorator(KubernetesMasterConf kubernetesMasterConf) {
		super(kubernetesMasterConf.getFlinkConfiguration());
		this.kubernetesMasterConf = kubernetesMasterConf;
	}

	@Override
	public FlinkPod configureFlinkPod(FlinkPod flinkPod) {

		final String startCommand = getJobManagerStartCommand(
			configuration,
			kubernetesMasterConf.getJobManagerMemoryMB(),
			kubernetesMasterConf.getInternalFlinkConfDir(),
			kubernetesMasterConf.getInternalFlinkLogDir(),
			kubernetesMasterConf.hasLogback(),
			kubernetesMasterConf.hasLog4j(),
			kubernetesMasterConf.getEntrypointMainClass(),
			null);

		final Container containerWithStartCommand = new ContainerBuilder(flinkPod.getMainContainer())
			.withCommand(kubernetesMasterConf.getInternalEntrypoint())
			.withArgs(Arrays.asList("/bin/bash", "-c", startCommand))
			.build();

		return new FlinkPodBuilder(flinkPod)
			.withNewMainContainer(containerWithStartCommand)
			.build();
	}

	/**
	 * Generates the shell command to start a job manager for kubernetes.
	 *
	 * @param flinkConfig The Flink configuration.
	 * @param jobManagerMemoryMb JobManager heap size.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @param mainArgs The args for main class.
	 * @return A String containing the job manager startup command.
	 */
	public static String getJobManagerStartCommand(
			Configuration flinkConfig,
			int jobManagerMemoryMb,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			String mainClass,
			@Nullable String mainArgs) {
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
			mainArgs);
	}
}
