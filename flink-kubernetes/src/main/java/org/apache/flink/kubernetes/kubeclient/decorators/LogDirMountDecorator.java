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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

/**
 * .
 */
public class LogDirMountDecorator extends AbstractKubernetesStepDecorator {

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Volume volume = new VolumeBuilder()
			.withName("log")
			.withHostPath(new HostPathVolumeSourceBuilder()
				.withPath("/tmp/flink/log")
				.withType("Directory")
				.build())
			.build();

		final VolumeMount volumeMount = new VolumeMountBuilder()
			.withName("log")
			.withMountPath("/opt/flink/log")
			.build();

		final Pod podWithLogVolume = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToVolumes(volume)
				.endSpec()
			.build();

		final Container mainContainerWithLogVolumeMount = new ContainerBuilder(flinkPod.getMainContainer())
			.addToVolumeMounts(volumeMount)
			.build();

		return new FlinkPod.Builder(flinkPod)
			.withPod(podWithLogVolume)
			.withMainContainer(mainContainerWithLogVolumeMount)
			.build();
	}
}
