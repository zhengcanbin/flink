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

package org.apache.flink.kubernetes.kubeclient;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;

import java.util.List;

/**
 *	Composition of the created Kubernetes components that represents a Flink application.
 */
public class KubernetesJobManagerSpecification {

	private Deployment deployment;

	private List<ConfigMap> accompanyingConfigMaps;

	private List<Service> accompanyingServices;

	public KubernetesJobManagerSpecification(Deployment deployment,
			List<ConfigMap> accompanyingConfigMaps,
			List<Service> accompanyingServices) {
		this.deployment = deployment;
		this.accompanyingConfigMaps = accompanyingConfigMaps;
		this.accompanyingServices = accompanyingServices;
	}

	public Deployment getDeployment() {
		return deployment;
	}

	public List<ConfigMap> getAccompanyingConfigMaps() {
		return accompanyingConfigMaps;
	}

	public List<Service> getAccompanyingServices() {
		return accompanyingServices;
	}
}
