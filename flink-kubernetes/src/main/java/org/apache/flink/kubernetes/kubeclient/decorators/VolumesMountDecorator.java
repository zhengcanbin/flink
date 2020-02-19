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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameter.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

/**
 * Mounts the user-specified volumes to the JobManager or TaskManager pod.
 */
public class VolumesMountDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesParameters kubernetesComponentConf;

	public VolumesMountDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
		super(kubernetesComponentConf.getFlinkConfiguration());
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Tuple2<Volume[], VolumeMount[]> volumes = kubernetesComponentConf.getVolumes();

		final Pod podWithVolumes = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToVolumes(volumes.f0)
				.endSpec()
			.build();

		final Container containerWithVolumeMounts = new ContainerBuilder(flinkPod.getMainContainer())
			.addToVolumeMounts(volumes.f1)
			.build();

		return new FlinkPod(podWithVolumes, containerWithVolumeMounts);
	}
}
