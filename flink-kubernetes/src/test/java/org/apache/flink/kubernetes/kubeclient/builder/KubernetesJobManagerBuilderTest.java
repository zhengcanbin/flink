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

package org.apache.flink.kubernetes.kubeclient.builder;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.KubernetesMasterSpecification;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KubernetesJobManagerBuilderTest {

	private static final String _CLUSTER_ID = "cluster-id-test";
	private static final String _NAMESPACE = "default-test";
	private static final String _CONTAINER_IMAGE = "flink:latest";
	private static final String _CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

	private static final int _REST_PORT = 9081;
	private static final int _RPC_PORT = 7123;
	private static final int _BLOB_SERVER_PORT = 8346;

	private static final double _JOB_MANAGER_CPU = 2.0;
	private static final int _JOB_MANAGER_MEMORY = 768;

	private static final String _SERVICE_ACCOUNT_NAME = "service-test";

	private static final String _ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private final Map<String, String> customizedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private KubernetesMasterSpecification kubernetesMasterSpecification;

	@Before
	public void setup() throws IOException {
		final File flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		final Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Configuration flinkConfig = new Configuration();
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, _CLUSTER_ID);
		flinkConfig.set(KubernetesConfigOptions.NAMESPACE, _NAMESPACE);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, _CONTAINER_IMAGE);
		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, _CONTAINER_IMAGE_PULL_POLICY);
		flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, _ENTRY_POINT_CLASS);
		flinkConfig.set(RestOptions.PORT, _REST_PORT);
		flinkConfig.set(JobManagerOptions.PORT, _RPC_PORT);
		flinkConfig.set(BlobServerOptions.PORT, Integer.toString(_BLOB_SERVER_PORT));
		flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, _JOB_MANAGER_CPU);
		flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, _SERVICE_ACCOUNT_NAME);
		customizedEnvs.forEach((k, v) ->
			flinkConfig.setString(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + k, v));

		final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(_JOB_MANAGER_MEMORY)
			.setTaskManagerMemoryMB(1000)
			.setSlotsPerTaskManager(3)
			.createClusterSpecification();

		final KubernetesMasterConf kubernetesMasterConf = new KubernetesMasterConf(flinkConfig, clusterSpecification);

		this.kubernetesMasterSpecification = KubernetesJobManagerBuilder.buildJobManagerComponent(kubernetesMasterConf);
	}

	@Test
	public void testDeploymentMetadata() throws IOException {
		final Deployment deployment = this.kubernetesMasterSpecification.getDeployment();
		assertEquals(_CLUSTER_ID, deployment.getMetadata().getName());

		final Map<String, String> commonLabels =  new HashMap<>();
		commonLabels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		commonLabels.put(Constants.LABEL_APP_KEY, _CLUSTER_ID);
		assertEquals(commonLabels, deployment.getMetadata().getLabels());
	}

	@Test
	public void testDeploymentSpec() {
		final DeploymentSpec deploymentSpec = this.kubernetesMasterSpecification.getDeployment().getSpec();
		assertEquals(1, deploymentSpec.getReplicas().intValue());

		final Map<String, String> expectedLabels =  new HashMap<>();
		expectedLabels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		expectedLabels.put(Constants.LABEL_APP_KEY, _CLUSTER_ID);
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

		assertEquals(expectedLabels, deploymentSpec.getTemplate().getMetadata().getLabels());
		assertEquals(expectedLabels, deploymentSpec.getSelector().getMatchLabels());

		assertNotNull(deploymentSpec.getTemplate().getSpec());
	}

	@Test
	public void testPodSpec() {
		final PodSpec resultedPodSpec = this.kubernetesMasterSpecification.getDeployment().getSpec().getTemplate().getSpec();

		assertEquals(1, resultedPodSpec.getContainers().size());
		assertEquals(_SERVICE_ACCOUNT_NAME, resultedPodSpec.getServiceAccountName());
		assertEquals(1, resultedPodSpec.getVolumes().size());

		final Container resultedMainContainer = resultedPodSpec.getContainers().get(0);
		assertEquals(KubernetesMasterConf.JOB_MANAGER_MAIN_CONTAINER_NAME, resultedMainContainer.getName());
		assertEquals(_CONTAINER_IMAGE, resultedMainContainer.getImage());
		assertEquals(_CONTAINER_IMAGE_PULL_POLICY, resultedMainContainer.getImagePullPolicy());

		assertEquals(3, resultedMainContainer.getEnv().size());
		assertTrue(resultedMainContainer.getEnv()
				.stream()
				.anyMatch(envVar -> envVar.getName().equals("key1")));

		assertEquals(3, resultedMainContainer.getPorts().size());

		final Map<String, Quantity> requests = resultedMainContainer.getResources().getRequests();
		assertEquals(Double.toString(_JOB_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(_JOB_MANAGER_MEMORY + "Mi", requests.get("memory").getAmount());

		assertEquals(1, resultedMainContainer.getCommand().size());
		assertEquals(3, resultedMainContainer.getArgs().size());

		assertEquals(1, resultedMainContainer.getVolumeMounts().size());
	}

	@Test
	public void testAdditionalResourcesSize() {
		final List<HasMetadata> resultedAdditionalResources = this.kubernetesMasterSpecification.getAdditionalResources();
		assertEquals(2, resultedAdditionalResources.size());

		final List<HasMetadata> resultedServices = resultedAdditionalResources
			.stream()
			.filter(x -> x instanceof Service)
			.collect(Collectors.toList());
		assertEquals(1, resultedServices.size());

		final List<HasMetadata> resultedConfigMaps = resultedAdditionalResources
			.stream()
			.filter(x -> x instanceof ConfigMap)
			.collect(Collectors.toList());
		assertEquals(1, resultedConfigMaps.size());
	}

	@Test
	public void testService() {
		final Service resultedService = (Service) this.kubernetesMasterSpecification.getAdditionalResources()
			.stream()
			.filter(x -> x instanceof Service)
			.collect(Collectors.toList())
			.get(0);

		assertEquals(resultedService.getMetadata().getName(), KubernetesUtils.getRestServiceName(_CLUSTER_ID));
		assertEquals(2, resultedService.getMetadata().getLabels().size());

		assertEquals(resultedService.getSpec().getType(), "LoadBalancer");
		assertEquals(3, resultedService.getSpec().getPorts().size());
		assertEquals(3, resultedService.getSpec().getSelector().size());
	}

	@Test
	public void testFlinkConfConfigMap() {
		final ConfigMap resultedConfigMap = (ConfigMap) this.kubernetesMasterSpecification.getAdditionalResources()
			.stream()
			.filter(x -> x instanceof ConfigMap)
			.collect(Collectors.toList())
			.get(0);

		assertEquals(2, resultedConfigMap.getMetadata().getLabels().size());

		final Map<String, String> resultedDataMap = resultedConfigMap.getData();
		assertEquals(3, resultedDataMap.size());
		assertEquals("some data", resultedDataMap.get("log4j.properties"));
		assertEquals("some data", resultedDataMap.get("logback.xml"));
		// todo compare the flink configuration datas

	}

}
