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

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class RestServiceDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesMasterConf kubernetesMasterConf;

	public RestServiceDecorator(KubernetesMasterConf kubernetesMasterConf) {
		super(kubernetesMasterConf.getFlinkConfiguration());
		this.kubernetesMasterConf = kubernetesMasterConf;
	}

	@Override
	public List<HasMetadata> generateAdditionalKubernetesResources() throws IOException {
		final String clusterId = kubernetesMasterConf.getClusterId();
		final String restServiceName = KubernetesUtils.getRestServiceName(clusterId);

		// Set jobmanager address to namespaced service name
		final String namespace = kubernetesMasterConf.getNamespace();
		configuration.setString(JobManagerOptions.ADDRESS, restServiceName + "." + namespace);

		final Service restService = new ServiceBuilder()
			.withNewMetadata()
				.withName(restServiceName)
				.withLabels(kubernetesMasterConf.getCommonLabels())
				.endMetadata()
			.withNewSpec()
				.withType(kubernetesMasterConf.getRestServiceExposedType())
				.withPorts(getServicePorts())
				.withSelector(kubernetesMasterConf.getLabels())
				.endSpec()
			.build();

		return Collections.singletonList(restService);
	}

	private List<ServicePort> getServicePorts() {
		final List<ServicePort> servicePorts = new ArrayList<>();
		servicePorts.add(getServicePort(
			getPortName(RestOptions.PORT.key()),
			kubernetesMasterConf.getRestPort()));
		servicePorts.add(getServicePort(
			getPortName(JobManagerOptions.PORT.key()),
			kubernetesMasterConf.getRPCPort()));
		servicePorts.add(getServicePort(
			getPortName(BlobServerOptions.PORT.key()),
			kubernetesMasterConf.getBlobServerPort()));

		return servicePorts;
	}

	private ServicePort getServicePort(String name, int port) {
		return new ServicePortBuilder()
			.withName(name)
			.withPort(port)
			.build();
	}

	private String getPortName(String portName){
		return portName.replace('.', '-');
	}
}
