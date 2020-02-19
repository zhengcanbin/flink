package org.apache.flink.kubernetes.kubeclient.resources;

import io.fabric8.kubernetes.api.model.Service;

/**
 * Represent Service resource in kubernetes.
 */
public class KubernetesService extends KubernetesResource<Service> {

	public KubernetesService(Service internalResource) {
		super(internalResource);
	}
}
