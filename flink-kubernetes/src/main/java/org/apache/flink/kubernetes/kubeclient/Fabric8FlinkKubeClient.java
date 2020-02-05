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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
	public void createFlinkMasterComponent(KubernetesMasterSpecification spec) {
		final Deployment deployment = spec.getDeployment();
		final List<HasMetadata> additionalResources = spec.getAdditionalResources();

		// create Deployment
		final Deployment createdDeployment = this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.create(deployment);

		// set Deployment as the OwnerReference of all other Resources, including ConfigMaps, Services.
		setOwnerReference(createdDeployment, additionalResources);

		// create other Resources, including ConfigMaps, Services.
		this.internalClient
			.resourceList(additionalResources)
			.inNamespace(this.nameSpace)
			.createOrReplace();
	}

	@Override
	public void createTaskManagerPod(KubernetesPod pod) {
		final Deployment masterDeployment = this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.withName(clusterId)
			.get();

		if (masterDeployment == null) {
			// todo throw Exception
			return;
		}

		setOwnerReference(masterDeployment, Collections.singletonList(pod.getInternalResource()));

		this.internalClient
			.pods()
			.inNamespace(this.nameSpace)
			.create(pod.getInternalResource());
	}

	private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
		final OwnerReference deploymentOwnerReference = new OwnerReferenceBuilder()
			.withName(deployment.getMetadata().getName())
			.withApiVersion(deployment.getApiVersion())
			.withUid(deployment.getMetadata().getUid())
			.withKind(deployment.getKind())
			.withController(true)
			.build();
		resources.forEach(resource -> {
			resource.getMetadata().setOwnerReferences(Collections.singletonList(deploymentOwnerReference));
		});
	}

	@Override
	public void stopPod(String podName) {
		this.internalClient.pods().withName(podName).delete();
	}

	@Override
	@Nullable
	public Endpoint getRestEndpoint(String clusterId) {
		int restPort = this.flinkConfig.getInteger(RestOptions.PORT);
		String serviceExposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);

		// Return the service.namespace directly when use ClusterIP.
		if (serviceExposedType.equals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString())) {
			return new Endpoint(KubernetesUtils.getRestServiceName(clusterId) + "." + nameSpace, restPort);
		}

		Service service = getRestService(clusterId);

		String address = null;

		if (service.getStatus() != null && (service.getStatus().getLoadBalancer() != null ||
			service.getStatus().getLoadBalancer().getIngress() != null)) {
			if (service.getStatus().getLoadBalancer().getIngress().size() > 0) {
				address = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
				if (address == null || address.isEmpty()) {
					address = service.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
				}
			} else {
				address = this.internalClient.getMasterUrl().getHost();
				restPort = getServiceNodePort(service, RestOptions.PORT);
			}
		} else if (service.getSpec().getExternalIPs() != null && service.getSpec().getExternalIPs().size() > 0) {
			address = service.getSpec().getExternalIPs().get(0);
		}
		if (address == null || address.isEmpty()) {
			return null;
		}
		return new Endpoint(address, restPort);
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
		this.internalClient.services().inNamespace(this.nameSpace).withName(clusterId).cascading(true).delete();
	}

	@Override
	public void handleException(Exception e) {
		LOG.error("Encounter Kubernetes Exception.", e);
	}

	@Override
	@Nullable
	public Service getRestService(String clusterId) {
		final String restServiceName = KubernetesUtils.getRestServiceName(clusterId);
		final Service restService = this
			.internalClient
			.services()
			.inNamespace(nameSpace)
			.withName(restServiceName)
			.fromServer()
			.get();

		if (restService == null) {
			LOG.debug("Service {} does not exist", restServiceName);
			return null;
		}

		return restService;
	}

	@Override
	public void watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler callbackHandler) {
		final Watcher<Pod> watcher = new Watcher<Pod>() {
			@Override
			public void eventReceived(Action action, Pod pod) {
				LOG.info("Received {} event for pod {}, details: {}", action, pod.getMetadata().getName(), pod.getStatus());
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
						LOG.info("Skip handling {} event for pod {}", action, pod.getMetadata().getName());
						break;
				}
			}

			@Override
			public void onClose(KubernetesClientException e) {
				LOG.error("Pods watcher onClose", e);
			}
		};
		this.internalClient.pods().withLabels(labels).watch(watcher);
	}

	@Override
	public void close() {
		this.internalClient.close();
	}

	/**
	 * To get nodePort of configured ports.
	 */
	private int getServiceNodePort(Service service, ConfigOption<Integer> configPort) {
		final int port = this.flinkConfig.getInteger(configPort);
		if (service.getSpec() != null && service.getSpec().getPorts() != null) {
			for (ServicePort p : service.getSpec().getPorts()) {
				if (p.getPort() == port) {
					return p.getNodePort();
				}
			}
		}
		return port;
	}
}
