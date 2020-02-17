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
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesTaskManagerConf;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * Creates the command and args for the main container to run the TaskManager code.
 */
public class JavaCmdTaskManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesTaskManagerConf kubernetesTaskManagerConf;

	public JavaCmdTaskManagerDecorator(KubernetesTaskManagerConf kubernetesTaskManagerConf) {
		super(kubernetesTaskManagerConf.getFlinkConfiguration());
		this.kubernetesTaskManagerConf = kubernetesTaskManagerConf;
	}

	@Override
	protected Container decorateMainContainer(Container container) {
		return new ContainerBuilder(container)
				.withCommand(kubernetesTaskManagerConf.getInternalEntrypoint())
				.withArgs(Arrays.asList("/bin/bash", "-c", getTaskManagerStartCommand()))
				.build();
	}

	private String getTaskManagerStartCommand() {
		final String confDir = kubernetesTaskManagerConf.getInternalFlinkConfDir();

		final String logDir = kubernetesTaskManagerConf.getInternalFlinkLogDir();

		final String mainClassArgs = "--" + CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt() + " " +
			confDir + " " + kubernetesTaskManagerConf.getDynamicProperties();

		return getTaskManagerStartCommand(
			kubernetesTaskManagerConf.getFlinkConfiguration(),
			kubernetesTaskManagerConf.getContaineredTaskManagerParameters(),
			confDir,
			logDir,
			kubernetesTaskManagerConf.hasLogback(),
			kubernetesTaskManagerConf.hasLog4j(),
			KubernetesTaskExecutorRunner.class.getCanonicalName(),
			mainClassArgs);
	}

	private static String getTaskManagerStartCommand(
			Configuration flinkConfig,
			ContaineredTaskManagerParameters tmParams,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			String mainClass,
			@Nullable String mainArgs) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec = tmParams.getTaskExecutorProcessSpec();
		final String jvmMemOpts = TaskExecutorProcessUtils.generateJvmParametersStr(taskExecutorProcessSpec);
		String args = TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec);
		if (mainArgs != null) {
			args += " " + mainArgs;
		}

		return KubernetesUtils.getCommonStartCommand(
			flinkConfig,
			KubernetesUtils.ClusterComponent.TASK_MANAGER,
			jvmMemOpts,
			configDirectory,
			logDirectory,
			hasLogback,
			hasLog4j,
			mainClass,
			args);
	}
}
