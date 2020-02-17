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
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link ExternalServiceDecorator}.
 */
public class ExternalServiceDecoratorTest extends JobManagerDecoratorTest {
	private static final String _NAMESPACE = "default-test";

	private static final int _REST_PORT = 9081;
	private static final int _RPC_PORT = 7123;
	private static final int _BLOB_SERVER_PORT = 8346;

	private ExternalServiceDecorator externalServiceDecorator;

	@Before
	public void setup() throws IOException {
		super.setup();

		this.flinkConfig.set(KubernetesConfigOptions.NAMESPACE, _NAMESPACE);
		this.flinkConfig.set(RestOptions.PORT, _REST_PORT);
		this.flinkConfig.set(JobManagerOptions.PORT, _RPC_PORT);
		this.flinkConfig.set(BlobServerOptions.PORT, Integer.toString(_BLOB_SERVER_PORT));

		this.externalServiceDecorator = new ExternalServiceDecorator(this.kubernetesJobManagerConf);
	}

	@Test
	public void testBuildAccompanyingKubernetesResources() throws IOException {
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, resources.size());

		final Service restService = (Service) resources.get(0);

		assertEquals(KubernetesUtils.getRestServiceName(_CLUSTER_ID), restService.getMetadata().getName());

		final Map<String, String> expectedLabels = new HashMap<>();
		expectedLabels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		expectedLabels.put(Constants.LABEL_APP_KEY, _CLUSTER_ID);
		assertEquals(expectedLabels, restService.getMetadata().getLabels());

		assertEquals("LoadBalancer", restService.getSpec().getType());

		List<ServicePort> expectedServicePorts = Collections.singletonList(
			new ServicePortBuilder()
				.withName("rest-port")
				.withPort(_REST_PORT)
				.build());
		assertEquals(expectedServicePorts, restService.getSpec().getPorts());

		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		assertEquals(expectedLabels, restService.getSpec().getSelector());
	}

	@Test
	public void testSetServiceExposedType() throws IOException {
		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, "NodePort");
		List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals("NodePort", ((Service) resources.get(0)).getSpec().getType());

		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, "ClusterIP");
		resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals("ClusterIP", ((Service) resources.get(0)).getSpec().getType());
	}
}
