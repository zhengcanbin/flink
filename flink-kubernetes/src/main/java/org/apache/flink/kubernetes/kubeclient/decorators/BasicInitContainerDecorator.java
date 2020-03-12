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
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the init Container for the JobManager/TaskManager in per-job deployment.
 */
public class BasicInitContainerDecorator extends AbstractKubernetesStepDecorator {
	private final AbstractKubernetesParameters kubernetesParameters;

	public BasicInitContainerDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		if (!kubernetesParameters.runInitContainer()) {
			return flinkPod;
		}

		final VolumeMount[] volumeMounts = new VolumeMount[] {
			new VolumeMountBuilder()
				.withName(Constants.DOWNLOAD_JARS_VOLUME_NAME)
				.withMountPath(kubernetesParameters.getJarsDownloadDir())
				.build(),
			new VolumeMountBuilder()
				.withName(Constants.DOWNLOAD_FILES_VOLUME_NAME)
				.withMountPath(kubernetesParameters.getFilesDownloadDir())
				.build()
		};

		final Pod podWithVolume = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addNewVolume()
					.withName(Constants.DOWNLOAD_JARS_VOLUME_NAME)
					.withEmptyDir(new EmptyDirVolumeSource())
					.endVolume()
				.addNewVolume()
					.withName(Constants.DOWNLOAD_FILES_VOLUME_NAME)
					.withEmptyDir(new EmptyDirVolumeSource())
					.endVolume()
				.endSpec()
			.build();

		final Container initContainer = new ContainerBuilder(flinkPod.getInitContainer())
			.withName(kubernetesParameters.getInitContainerName())
			.withImage(kubernetesParameters.getImage())
			.withImagePullPolicy(kubernetesParameters.getImagePullPolicy())
			// todo environment
			.addToVolumeMounts(volumeMounts)
			.build();

		final Container mainContainer = new ContainerBuilder(flinkPod.getMainContainer())
			.addToVolumeMounts(volumeMounts)
			.build();

		return new FlinkPod.Builder()
			.withPod(podWithVolume)
			.withInitContainer(initContainer)
			.withMainContainer(mainContainer)
			.build();
	}
}
