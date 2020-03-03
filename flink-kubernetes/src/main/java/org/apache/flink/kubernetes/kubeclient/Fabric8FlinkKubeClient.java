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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TimeUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClient implements FlinkKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(Fabric8FlinkKubeClient.class);

	private final Configuration flinkConfig;
	private final KubernetesClient internalClient;
	private final String clusterId;
	private final String nameSpace;

	public Fabric8FlinkKubeClient(Configuration flinkConfig, KubernetesClient client) {
		this.flinkConfig = checkNotNull(flinkConfig);
		this.internalClient = checkNotNull(client);
		this.clusterId = checkNotNull(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID));

		this.nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
	}

	@Override
	public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
		final Deployment deployment = kubernetesJMSpec.getDeployment();
		final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

		// create Deployment
		LOG.debug("Start to create deployment with spec {}", deployment.getSpec().toString());
		final Deployment createdDeployment = this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.create(deployment);

		// Note that we should use the uid of the created Deployment for the OwnerReference.
		setOwnerReference(createdDeployment, accompanyingResources);

		this.internalClient
			.resourceList(accompanyingResources)
			.inNamespace(this.nameSpace)
			.createOrReplace();
	}

	@Override
	public void createTaskManagerPod(KubernetesPod kubernetesPod) {
		final Deployment masterDeployment = this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.withName(KubernetesUtils.getDeploymentName(clusterId))
			.get();

		if (masterDeployment == null) {
			throw new RuntimeException(
				"Failed to find Deployment named " + clusterId + " in namespace " + this.nameSpace);
		}

		// Note that we should use the uid of the master Deployment for the OwnerReference.
		setOwnerReference(masterDeployment, Collections.singletonList(kubernetesPod.getInternalResource()));

		LOG.debug("Start to create pod with metadata {}, spec {}",
			kubernetesPod.getInternalResource().getMetadata(),
			kubernetesPod.getInternalResource().getSpec());

		this.internalClient
			.pods()
			.inNamespace(this.nameSpace)
			.create(kubernetesPod.getInternalResource());
	}

	@Override
	public void stopPod(String podName) {
		this.internalClient.pods().withName(podName).delete();
	}

	@Override
	@Nullable
	public CompletableFuture<Optional<Endpoint>> getRestEndpoint(String clusterId) {
		return CompletableFuture.supplyAsync(() -> {
				Optional<Endpoint> endpoint = getRestEndpointInternal(clusterId);

				final Duration timeout =
					TimeUtils.parseDuration(flinkConfig.get(KubernetesConfigOptions.SERVICE_CREATE_TIMEOUT));
				final long endTime = timeout.toMillis() + System.currentTimeMillis();

				while (!endpoint.isPresent() && System.currentTimeMillis() <= endTime) {
					endpoint = getRestEndpointInternal(clusterId);
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// do nothing
					}
					LOG.debug("endpoint = " + endpoint.toString());
				}

				return endpoint;
			});
	}

	private Optional<Endpoint> getRestEndpointInternal(String clusterId) {
		final KubernetesService wrapRestService = getRestService(clusterId);
		if (wrapRestService == null) {
			throw new RuntimeException("Failed to find Service via clusterId " + clusterId);
		}
		final Service restService = wrapRestService.getInternalResource();

		final KubernetesConfigOptions.ServiceExposedType restServiceType =
			KubernetesConfigOptions.ServiceExposedType.valueOf(restService.getSpec().getType());

		final List<ServicePort> restServicePortCandidates = restService.getSpec().getPorts()
			.stream()
			.filter(x -> x.getName().equals(Constants.REST_PORT_NAME))
			.collect(Collectors.toList());

		if (restServicePortCandidates.isEmpty()) {
			throw new RuntimeException("Failed to find port " + Constants.REST_PORT_NAME + " in Service " +
				KubernetesUtils.getRestServiceName(this.clusterId));
		}
		final ServicePort restServicePort = restServicePortCandidates.get(0);

		String address = null;
		int restPort = restServicePort.getPort();
		switch (restServiceType) {
			case ClusterIP:
				address = KubernetesUtils.getRestServiceName(clusterId) + "." + nameSpace;
				break;
			case NodePort:
				final List<Node> nodes = this.internalClient.nodes().list().getItems();
				final int randomNodeIndex = new Random().nextInt(nodes.size());
				final List<NodeAddress> nodeAddresses = nodes.get(randomNodeIndex).getStatus().getAddresses();
				for (NodeAddress nodeAddress: nodeAddresses) {
					if (nodeAddress.getType().equals("InternalIP")) {
						address = nodeAddress.getAddress();
						break;
					}
				}
				restPort = restServicePort.getNodePort();
				break;
			case LoadBalancer:
				final List<LoadBalancerIngress> ingresses = restService.getStatus().getLoadBalancer().getIngress();
				if (!ingresses.isEmpty()) {
					address = ingresses.get(0).getIp();
					if (StringUtils.isNullOrWhitespaceOnly(address)) {
						address = ingresses.get(0).getHostname();
					}
					if (StringUtils.isNullOrWhitespaceOnly(address)) {
						address = null;
					}
				} else {
					LOG.debug("LoadBalancerIngress is not found");
				}
				break;
			default:
				throw new RuntimeException("Unrecognized Service type: " + restServiceType);
		}

		if (address == null) {
			return Optional.empty();
		}

		return Optional.of(new Endpoint(address, restPort));
	}

	@Override
	public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
		final List<Pod> podList = this.internalClient.pods().withLabels(labels).list().getItems();

		if (podList == null || podList.size() < 1) {
			return new ArrayList<>();
		}

		return podList
			.stream()
			.map(KubernetesPod::new)
			.collect(Collectors.toList());
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.withName(KubernetesUtils.getDeploymentName(clusterId))
			.cascading(true)
			.delete();
	}

	@Override
	public void handleException(Exception e) {
		LOG.error("A Kubernetes exception occurred.", e);
	}

	@Override
	@Nullable
	public KubernetesService getRestService(String clusterId) {
		final String serviceName = KubernetesUtils.getRestServiceName(clusterId);

		final Service service = this.internalClient
			.services()
			.inNamespace(nameSpace)
			.withName(serviceName)
			.fromServer()
			.get();

		if (service == null) {
			LOG.debug("Service {} does not exist", serviceName);
			return null;
		}

		return new KubernetesService(service);
	}

	@Override
	public void watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler callbackHandler) {
		final Watcher<Pod> watcher = new Watcher<Pod>() {
			@Override
			public void eventReceived(Action action, Pod pod) {
				LOG.debug("Received {} event for pod {}, details: {}", action, pod.getMetadata().getName(), pod.getStatus());
				switch (action) {
					case ADDED:
						callbackHandler.onAdded(Collections.singletonList(new KubernetesPod(pod)));
						break;
					case MODIFIED:
						callbackHandler.onModified(Collections.singletonList(new KubernetesPod(pod)));
						break;
					case ERROR:
						callbackHandler.onError(Collections.singletonList(new KubernetesPod(pod)));
						break;
					case DELETED:
						callbackHandler.onDeleted(Collections.singletonList(new KubernetesPod(pod)));
						break;
					default:
						LOG.debug("Ignore handling {} event for pod {}", action, pod.getMetadata().getName());
						break;
				}
			}

			@Override
			public void onClose(KubernetesClientException e) {
				LOG.error("The pods watcher is closing.", e);
			}
		};
		this.internalClient.pods().withLabels(labels).watch(watcher);
	}

	@Override
	public void close() {
		this.internalClient.close();
	}

	private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
		final OwnerReference deploymentOwnerReference = new OwnerReferenceBuilder()
			.withName(deployment.getMetadata().getName())
			.withApiVersion(deployment.getApiVersion())
			.withUid(deployment.getMetadata().getUid())
			.withKind(deployment.getKind())
			.withController(true)
			.withBlockOwnerDeletion(true)
			.build();
		resources.forEach(resource ->
			resource.getMetadata().setOwnerReferences(Collections.singletonList(deploymentOwnerReference)));
	}
}
