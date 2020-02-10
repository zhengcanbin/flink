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
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesVolumeUtils;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.util.Map;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_JOBMANAGER_VOLUMES_PREFIX;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 */
public class KubernetesMasterConf extends AbstractKubernetesComponentConf {

	public static final String JOB_MANAGER_MAIN_CONTAINER_NAME = "flink-job-manager";

	private final ClusterSpecification clusterSpecification;

	public KubernetesMasterConf(Configuration flinkConfig, ClusterSpecification clusterSpecification) {
		super(flinkConfig);
		this.clusterSpecification = clusterSpecification;
	}

	@Override
	public Map<String, String> getLabels() {
		Map<String, String> labels = getCommonLabels();
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

		return labels;
	}

	@Override
	public Map<String, String> getEnvironments() {
		return getPrefixedEnvironments(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX);
	}

	@Override
	public Tuple2<Volume[], VolumeMount[]> getVolumes() {
		return KubernetesVolumeUtils.constructVolumes(
			KubernetesVolumeUtils.parseVolumesWithPrefix(
				flinkConfig,
				KUBERNETES_JOBMANAGER_VOLUMES_PREFIX));
	}

	public String getJobManagerMainContainerName() {
		return JOB_MANAGER_MAIN_CONTAINER_NAME;
	}

	public int getJobManagerMemoryMB() {
		return clusterSpecification.getMasterMemoryMB();
	}

	public double getJobManagerCPU() {
		return flinkConfig.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU);
	}

	public int getRestPort() {
		return flinkConfig.getInteger(RestOptions.PORT);
	}

	public int getRPCPort() {
		return flinkConfig.getInteger(JobManagerOptions.PORT);
	}

	public int getBlobServerPort() {
		final int blobServerPort = KubernetesUtils.parsePort(flinkConfig, BlobServerOptions.PORT);
		checkArgument(blobServerPort > 0, "%s should not be 0.", BlobServerOptions.PORT.key());
		return blobServerPort;
	}

	public String getServiceAccount() {
		return flinkConfig.getString(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT);
	}

	public String getEntrypointMainClass() {
		final String mainClass = flinkConfig.getString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS);
		checkNotNull(mainClass, "Main class must be specified!");

		return mainClass;
	}

	public String getRestServiceExposedType() {
		final String exposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
		return KubernetesConfigOptions.ServiceExposedType.valueOf(exposedType).toString();
	}

}
