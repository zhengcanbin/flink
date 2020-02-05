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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

/**
 *
 */
public class FlinkPodBuilder {

	private FlinkPod flinkPod;

	public FlinkPodBuilder() {
		final Pod pod = new PodBuilder()
			.withNewMetadata()
				.endMetadata()
			.withNewSpec()
				.endSpec()
			.build();

		final Container mainContainer = new ContainerBuilder().build();

		this.flinkPod = new FlinkPod(pod, mainContainer);
	}

	public FlinkPodBuilder(FlinkPod flinkPod) {
		this.flinkPod = flinkPod;
	}

	public FlinkPodBuilder withNewPod(Pod pod) {
		this.flinkPod = new FlinkPod(pod, flinkPod.getMainContainer());
		return this;
	}

	public FlinkPodBuilder withNewMainContainer(Container mainContainer) {
		this.flinkPod = new FlinkPod(flinkPod.getPod(), mainContainer);
		return this;
	}

	public FlinkPod build() {
		return this.flinkPod;
	}

}
