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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.MixedKubernetesServer;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.builder.KubernetesJobManagerBuilder;
import org.apache.flink.kubernetes.kubeclient.parameter.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Fabric implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClientTest {

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

	@Rule
	public MixedKubernetesServer server = new MixedKubernetesServer(true, true);

	private KubernetesJobManagerSpecification kubernetesJobManagerSpecification;

	private KubernetesClient kubeClient;

	private FlinkKubeClient flinkKubeClient;

	@Before
	public void setUp() throws IOException {
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

		final KubernetesJobManagerParameters kubernetesJobManagerParameters = new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);

		this.kubernetesJobManagerSpecification = KubernetesJobManagerBuilder.buildJobManagerComponent(kubernetesJobManagerParameters);

		this.kubeClient = server.getClient();
		this.flinkKubeClient = new Fabric8FlinkKubeClient(flinkConfig, this.kubeClient);
	}

	@Test
	public void testCreateFlinkMasterComponent() throws Exception {
		mockInternalServiceAddEventFromServerSide();
		mockRestServiceAddEventFromServerSide();

		flinkKubeClient.createFlinkMasterComponent(this.kubernetesJobManagerSpecification);

		final List<Deployment> resultedDeployments = kubeClient.apps().deployments()
				.inNamespace(_NAMESPACE)
				.list()
				.getItems();
		assertEquals(1, resultedDeployments.size());

		final List<ConfigMap> resultedConfigMaps = kubeClient.configMaps()
				.inNamespace(_NAMESPACE)
				.list()
				.getItems();
		assertEquals(1, resultedConfigMaps.size());

		final List<Service> resultedServices = kubeClient.services()
				.inNamespace(_NAMESPACE)
				.list()
				.getItems();
		assertEquals(2, resultedServices.size());

		testOwnerReferenceSetting(resultedDeployments.get(0), resultedConfigMaps);
		testOwnerReferenceSetting(resultedDeployments.get(0), resultedServices);
	}

	private <T extends HasMetadata> void testOwnerReferenceSetting(
			HasMetadata ownerReference,
			List<T> resources) {
		resources.forEach(resource -> {
			List<OwnerReference> ownerReferences = resource.getMetadata().getOwnerReferences();
			assertEquals(1, ownerReferences.size());
			assertEquals(ownerReference.getMetadata().getUid(), ownerReferences.get(0).getUid());
		});
	}

	@Test
	public void testCreateFlinkTaskManagerPod() throws Exception {
		mockInternalServiceAddEventFromServerSide();
		mockRestServiceAddEventFromServerSide();

		this.flinkKubeClient.createFlinkMasterComponent(this.kubernetesJobManagerSpecification);

		final KubernetesPod kubernetesPod = new KubernetesPod(new PodBuilder()
				.editOrNewMetadata()
					.withName("mock-task-manager-pod")
					.endMetadata()
				.editOrNewSpec()
					.endSpec()
				.build());
		this.flinkKubeClient.createTaskManagerPod(kubernetesPod);

		final Pod resultTaskManagerPod =
				this.kubeClient.pods().inNamespace(_NAMESPACE).withName("mock-task-manager-pod").get();

		assertEquals(
				this.kubeClient.apps().deployments().inNamespace(_NAMESPACE).list().getItems().get(0).getMetadata().getUid(),
				resultTaskManagerPod.getMetadata().getOwnerReferences().get(0).getUid());
	}

	@Test
	public void testStopPod() {
		final String podName = "pod-for-delete";
		final Pod pod = new PodBuilder()
				.editOrNewMetadata()
					.withName(podName)
					.endMetadata()
				.editOrNewSpec()
					.endSpec()
				.build();

		this.kubeClient.pods().inNamespace(_NAMESPACE).create(pod);
		assertNotNull(this.kubeClient.pods().inNamespace(_NAMESPACE).withName(podName).get());

		this.flinkKubeClient.stopPod(podName);
		assertNull(this.kubeClient.pods().inNamespace(_NAMESPACE).withName(podName).get());
	}

	@Test
	public void testServiceLoadBalancerWithNoIP() {
		final String hostName = "test-host-name";
		mockRestServiceOnServerSide(_CLUSTER_ID, hostName, "");

		final Endpoint resultEndpoint = flinkKubeClient.getRestEndpoint(_CLUSTER_ID);

		assertEquals(hostName, resultEndpoint.getAddress());
		assertEquals(_REST_PORT, resultEndpoint.getPort());
	}

	@Test
	public void testServiceLoadBalancerEmptyHostAndIP() {
		mockRestServiceOnServerSide(_CLUSTER_ID, "", "");

		final Endpoint resultEndpoint1 = flinkKubeClient.getRestEndpoint(_CLUSTER_ID);
		assertNull(resultEndpoint1);
	}

	@Test
	public void testServiceLoadBalancerNullHostAndIP() {
		mockRestServiceOnServerSide(_CLUSTER_ID, null, null);

		final Endpoint resultEndpoint2 = flinkKubeClient.getRestEndpoint(_CLUSTER_ID);
		assertNull(resultEndpoint2);
	}

	@Test
	public void testStopAndCleanupCluster() throws Exception {
		mockInternalServiceAddEventFromServerSide();
		mockRestServiceAddEventFromServerSide();

		this.flinkKubeClient.createFlinkMasterComponent(this.kubernetesJobManagerSpecification);

		final KubernetesPod kubernetesPod = new KubernetesPod(new PodBuilder()
				.editOrNewMetadata()
				.withName("mock-task-manager-pod")
				.endMetadata()
				.editOrNewSpec()
				.endSpec()
				.build());
		this.flinkKubeClient.createTaskManagerPod(kubernetesPod);

		assertEquals(1, this.kubeClient.apps().deployments().inNamespace(_NAMESPACE).list().getItems().size());
		assertEquals(1, this.kubeClient.configMaps().inNamespace(_NAMESPACE).list().getItems().size());
		assertEquals(2, this.kubeClient.services().inNamespace(_NAMESPACE).list().getItems().size());
		assertEquals(1, this.kubeClient.pods().inNamespace(_NAMESPACE).list().getItems().size());

		this.flinkKubeClient.stopAndCleanupCluster(_CLUSTER_ID);
		assertTrue(this.kubeClient.apps().deployments().inNamespace(_NAMESPACE).list().getItems().isEmpty());
//		assertTrue(this.kubeClient.configMaps().inNamespace(_NAMESPACE).list().getItems().isEmpty());
//		assertTrue(this.kubeClient.services().inNamespace(_NAMESPACE).list().getItems().isEmpty());
//		assertTrue(this.kubeClient.pods().inNamespace(_NAMESPACE).list().getItems().isEmpty());
	}

	private void mockInternalServiceAddEventFromServerSide() {
		final Service mockService = new ServiceBuilder()
				.editOrNewMetadata()
				.endMetadata()
				.build();

		final String path = String.format("/api/v1/namespaces/%s/services?fieldSelector=metadata.name%%3D%s&watch=true",
				_NAMESPACE, KubernetesUtils.getInternalServiceName(_CLUSTER_ID));

		server.expect()
				.withPath(path)
				.andUpgradeToWebSocket()
				.open()
				.waitFor(1000)
				.andEmit(new WatchEvent(mockService, "ADDED"))
				.done()
				.once();
	}

	private void mockRestServiceAddEventFromServerSide() {
		final Service mockService = new ServiceBuilder()
				.editOrNewMetadata()
					.endMetadata()
				.build();

		final String path = String.format("/api/v1/namespaces/%s/services?fieldSelector=metadata.name%%3D%s&watch=true",
				_NAMESPACE, KubernetesUtils.getRestServiceName(_CLUSTER_ID));
		server.expect()
				.withPath(path)
				.andUpgradeToWebSocket()
				.open()
				.waitFor(1000)
				.andEmit(new WatchEvent(mockService, "ADDED"))
				.done()
				.once();
	}

	private void mockRestServiceOnServerSide(String clusterId, @Nullable String hostname, @Nullable String ip) {
		final String restServiceName = KubernetesUtils.getRestServiceName(clusterId);

		final String path = String.format("/api/v1/namespaces/%s/services/%s", _NAMESPACE, restServiceName);
		server.expect()
				.withPath(path)
				.andReturn(200, buildMockRestService(hostname, ip))
				.always();

		final Service resultService = kubeClient.services()
				.inNamespace(_NAMESPACE)
				.withName(KubernetesUtils.getRestServiceName(_CLUSTER_ID))
				.get();
		assertNotNull(resultService);
	}

	private Service buildMockRestService(@Nullable String hostname, @Nullable String ip) {
		final Service service = new ServiceBuilder()
				.build();

		service.setStatus(new ServiceStatusBuilder()
				.withLoadBalancer(new LoadBalancerStatus(Collections.singletonList(
						new LoadBalancerIngress(hostname, ip)))).build());

		return service;
	}

//	@Test
//	public void testCreateConfigMap() throws Exception {
//		flinkKubeClient.createConfigMap();
//
//		final List<ConfigMap> configMaps = kubeClient.configMaps().list().getItems();
//		assertEquals(1, configMaps.size());
//
//		// Check labels
//		final ConfigMap configMap = configMaps.get(0);
//		assertEquals(Constants.CONFIG_MAP_PREFIX + CLUSTER_ID, configMap.getMetadata().getName());
//		final Map<String, String> labels = getCommonLabels();
//		assertEquals(labels, configMap.getMetadata().getLabels());
//
//		// Check owner reference
//		assertEquals(1, configMap.getMetadata().getOwnerReferences().size());
//		assertEquals(MOCK_SERVICE_ID, configMap.getMetadata().getOwnerReferences().get(0).getUid());
//
//		// Check data
//		assertEquals(1, configMap.getData().size());
//		assertThat(configMap.getData().get(FLINK_CONF_FILENAME),
//			Matchers.containsString(KubernetesConfigOptions.CLUSTER_ID.key()));
//		assertThat(configMap.getData().get(FLINK_CONF_FILENAME),
//			Matchers.containsString(KubernetesConfigOptions.CONTAINER_IMAGE.key()));
//	}

//	@Test
//	public void testCreateRestService() throws Exception {
//		flinkKubeClient.createRestService(CLUSTER_ID).get();
//
//		final List<Service> services = kubeClient.services().list().getItems();
//		assertEquals(1, services.size());
//
//		final Service service = services.get(0);
//		assertEquals(CLUSTER_ID + Constants.FLINK_REST_SERVICE_SUFFIX, service.getMetadata().getName());
//		final Map<String, String> labels = getCommonLabels();
//		assertEquals(labels, service.getMetadata().getLabels());
//
//		assertEquals(1, service.getMetadata().getOwnerReferences().size());
//		assertEquals(MOCK_SERVICE_ID, service.getMetadata().getOwnerReferences().get(0).getUid());
//
//		assertEquals(KubernetesConfigOptions.ServiceExposedType.LoadBalancer.toString(), service.getSpec().getType());
//
//		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
//		assertEquals(labels, service.getSpec().getSelector());
//
//		assertThat(service.getSpec().getPorts().stream().map(ServicePort::getPort).collect(Collectors.toList()),
//			Matchers.hasItems(8081));
//
//		final Endpoint endpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
//		assertEquals(MOCK_SERVICE_IP, endpoint.getAddress());
//		assertEquals(8081, endpoint.getPort());
//	}
//
//	@Test
//	public void testCreateFlinkMasterDeployment() {
//		final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
//			.setMasterMemoryMB(1234)
//			.createClusterSpecification();
//
//		flinkKubeClient.createFlinkMasterDeployment(clusterSpecification);
//
//		final List<Deployment> deployments = kubeClient.apps().deployments().list().getItems();
//		assertEquals(1, deployments.size());
//
//		final Deployment deployment = deployments.get(0);
//		assertEquals(CLUSTER_ID, deployment.getMetadata().getName());
//		final Map<String, String> labels = getCommonLabels();
//		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
//		assertEquals(labels, deployment.getMetadata().getLabels());
//
//		assertEquals(1, deployment.getMetadata().getOwnerReferences().size());
//		assertEquals(MOCK_SERVICE_ID, deployment.getMetadata().getOwnerReferences().get(0).getUid());
//
//		final PodSpec jmPodSpec = deployment.getSpec().getTemplate().getSpec();
//		assertEquals("default", jmPodSpec.getServiceAccountName());
//		assertEquals(1, jmPodSpec.getVolumes().size());
//		assertEquals(1, jmPodSpec.getContainers().size());
//		final Container jmContainer = jmPodSpec.getContainers().get(0);
//
//		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
//			jmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
//		assertEquals(clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
//			jmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
//
//		assertThat(jmContainer.getPorts().stream().map(ContainerPort::getContainerPort).collect(Collectors.toList()),
//			Matchers.hasItems(8081, 6123, 6124));
//
//		assertEquals(1, jmContainer.getVolumeMounts().size());
//		final String mountPath = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
//		assertEquals(new File(mountPath, FLINK_CONF_FILENAME).getPath(),
//			jmContainer.getVolumeMounts().get(0).getMountPath());
//		assertEquals(FLINK_CONF_FILENAME, jmContainer.getVolumeMounts().get(0).getSubPath());
//
//		EnvVar masterEnv = new EnvVar(FLINK_MASTER_ENV_KEY, FLINK_MASTER_ENV_VALUE, null);
//		assertTrue(
//			"Environment " + masterEnv.toString() + " should be set.",
//			jmContainer.getEnv().contains(masterEnv));
//	}
//
//	@Test
//	public void testCreateTaskManagerPod() {
//		final String podName = "taskmanager-1";
//		final List<String> commands = Arrays.asList("/bin/bash", "-c", "start-command-of-taskmanager");
//		final int tmMem = 1234;
//		final double tmCpu = 1.2;
//		final Map<String, String> env = new HashMap<>();
//		env.put("RESOURCE_ID", podName);
//		TaskManagerPodParameter parameter = new TaskManagerPodParameter(
//			podName,
//			commands,
//			tmMem,
//			tmCpu,
//			env);
//		flinkKubeClient.createTaskManagerPod(parameter);
//
//		final List<Pod> pods = kubeClient.pods().list().getItems();
//		assertEquals(1, pods.size());
//
//		final Pod tmPod = pods.get(0);
//		assertEquals(podName, tmPod.getMetadata().getName());
//		final Map<String, String> labels = getCommonLabels();
//		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
//		assertEquals(labels, tmPod.getMetadata().getLabels());
//
//		assertEquals(1, tmPod.getMetadata().getOwnerReferences().size());
//		assertEquals(MOCK_SERVICE_ID, tmPod.getMetadata().getOwnerReferences().get(0).getUid());
//
//		assertEquals(1, tmPod.getSpec().getContainers().size());
//		final Container tmContainer = tmPod.getSpec().getContainers().get(0);
//		assertEquals(CONTAINER_IMAGE, tmContainer.getImage());
//		assertEquals(commands, tmContainer.getArgs());
//
//		assertEquals(tmMem + Constants.RESOURCE_UNIT_MB,
//			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
//		assertEquals(tmMem + Constants.RESOURCE_UNIT_MB,
//			tmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
//		assertEquals(String.valueOf(tmCpu),
//			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_CPU).getAmount());
//		assertEquals(String.valueOf(tmCpu),
//			tmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_CPU).getAmount());
//
//		assertThat(tmContainer.getEnv(), Matchers.contains(
//			new EnvVarBuilder().withName("RESOURCE_ID").withValue(podName).build()));
//
//		assertThat(tmContainer.getPorts().stream().map(ContainerPort::getContainerPort).collect(Collectors.toList()),
//			Matchers.hasItems(6122));
//
//		assertEquals(1, tmContainer.getVolumeMounts().size());
//		final String mountPath = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
//		assertEquals(new File(mountPath, FLINK_CONF_FILENAME).getPath(),
//			tmContainer.getVolumeMounts().get(0).getMountPath());
//		assertEquals(FLINK_CONF_FILENAME, tmContainer.getVolumeMounts().get(0).getSubPath());
//
//		// Stop the pod
//		flinkKubeClient.stopPod(podName);
//		assertEquals(0, kubeClient.pods().list().getItems().size());
//	}
}
