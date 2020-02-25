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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.FlinkPodBuilder;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * An abstract {@link KubernetesStepDecorator} contains common implementations for different plug-in features
 * while providing two additional methods.
 */
public abstract class AbstractKubernetesStepDecorator implements KubernetesStepDecorator {

	/**
	 * Apply transformations on the given FlinkPod in accordance to this feature.
	 * Note that we should return a FlinkPod that keeps all of the properties of the passed FlinkPod object.
	 */
	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Pod decoratedPod = this.decoratePod(flinkPod.getPod());
		final Container decoratedMainContainer = this.decorateMainContainer(flinkPod.getMainContainer());

		return new FlinkPodBuilder()
				.withPod(decoratedPod)
				.withMainContainer(decoratedMainContainer)
				.build();
	}

	/**
	 * Apply transformations on the given Pod in accordance to this feature.
	 * Note that we should return a Pod that keeps all of the properties of the passed Pod object.
	 *
	 * <p>So this is correct:
	 *
	 * <pre>
	 * {@code
	 * Pod decoratedPod = new PodBuilder(pod) // Keeps the original state
	 *     .editSpec()
	 *     ...
	 *     .build()
	 *
	 * return decoratedPod
	 * }
	 * </pre>
	 *
	 * <p>And this is the incorrect:
	 *
	 * <pre>
	 * {@code
	 * Pod decoratedPod = new PodBuilder() // Loses the original state
	 *     .editSpec()
	 *     ...
	 *     .build()
	 *
	 *   return decoratedPod
	 * }
	 * </pre>
	 */
	protected Pod decoratePod(Pod pod) {
		return pod;
	}

	/**
	 * Apply transformations on the given Container in accordance to this feature.
	 * Note that we should return a Container that keeps all of the properties of the passed Container object.
	 *
	 * <p>So this is correct:
	 *
	 * <pre>
	 * {@code
	 * Container decoratedContainer = new ContainerBuilder(container) // Keeps the original state
	 *     .editSpec()
	 *     ...
	 *     .build()
	 *
	 *   return decoratedContainer
	 * }
	 * </pre>
	 *
	 * <p>And this is the incorrect:
	 * <pre>
	 * {@code
	 * Container decoratedContainer = new ContainerBuilder() // Loses the original state
	 *     .withName()
	 *     ...
	 *     .build()
	 *
	 *   return decoratedContainer
	 * }
	 * </pre>
	 *
	 */
	protected Container decorateMainContainer(Container container) {
		return container;
	}

	@Override
	public List<ConfigMap> buildAccompanyingConfigMaps() throws IOException {
		return Collections.emptyList();
	}

	/**
	 * Note that the method could have a side effect of modifying the Flink Configuration object, such as
	 * update the JobManager address.
	 */
	@Override
	public List<Service> buildAccompanyingServices() {
		return Collections.emptyList();
	}
}
