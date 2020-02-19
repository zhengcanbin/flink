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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.kubeclient.parameter.KubernetesJobManagerParameters;

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
 * An abstract class containing some common implementations for the internal/external Services.
 */
public abstract class AbstractServiceDecorator extends AbstractKubernetesStepDecorator {

	protected final KubernetesJobManagerParameters kubernetesJobManagerParameters;

	public AbstractServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		super(kubernetesJobManagerParameters.getFlinkConfiguration());
		this.kubernetesJobManagerParameters = kubernetesJobManagerParameters;
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		final String serviceName = getServiceName();

		if (isInternalService()) {
			// Set jobmanager address to namespaced service name
			final String namespace = kubernetesJobManagerParameters.getNamespace();
			configuration.setString(JobManagerOptions.ADDRESS, serviceName + "." + namespace);
		}

		final Service restService = new ServiceBuilder()
			.withNewMetadata()
				.withName(getServiceName())
				.withLabels(kubernetesJobManagerParameters.getCommonLabels())
				.endMetadata()
			.withNewSpec()
				.withType(getServiceType())
				.withPorts(getServicePorts())
				.withSelector(kubernetesJobManagerParameters.getLabels())
				.endSpec()
			.build();

		return Collections.singletonList(restService);
	}

	protected abstract String getServiceType();

	protected abstract boolean isRestPortOnly();

	protected abstract String getServiceName();

	protected abstract boolean isInternalService();

	private List<ServicePort> getServicePorts() {
		final List<ServicePort> servicePorts = new ArrayList<>();
		servicePorts.add(getServicePort(
			getPortName(RestOptions.PORT.key()),
			kubernetesJobManagerParameters.getRestPort()));

		if (!isRestPortOnly()) {
			servicePorts.add(getServicePort(
				getPortName(JobManagerOptions.PORT.key()),
				kubernetesJobManagerParameters.getRPCPort()));
			servicePorts.add(getServicePort(
				getPortName(BlobServerOptions.PORT.key()),
				kubernetesJobManagerParameters.getBlobServerPort()));
		}

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
