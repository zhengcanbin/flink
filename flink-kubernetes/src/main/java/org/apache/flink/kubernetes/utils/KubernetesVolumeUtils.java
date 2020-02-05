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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.volumes.KubernetesEmptyDirVolumeConf;
import org.apache.flink.kubernetes.kubeclient.volumes.KubernetesHostPathVolumeConf;
import org.apache.flink.kubernetes.kubeclient.volumes.KubernetesPVCVolumeConf;
import org.apache.flink.kubernetes.kubeclient.volumes.KubernetesVolumeSpec;
import org.apache.flink.kubernetes.kubeclient.volumes.KubernetesVolumeSpecificConf;

import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_EMPTYDIR_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_HOSTPATH_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_PATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_READONLY_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_PATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_PVC_TYPE;


/**
 * Common utils for Kubernetes Volumes.
 */
public class KubernetesVolumeUtils {

	public static KubernetesVolumeSpec[] parseVolumesWithPrefix(Configuration flinkConfig, String prefix) {
		Map<String, String> properties = KubernetesUtils.parsePrefixedKVPairs(flinkConfig, prefix);

		return getVolumeTypesAndNames(properties).stream()
			.map(volumeTypeAndName -> {
				final String volumeType = volumeTypeAndName.f0;
				final String volumeName = volumeTypeAndName.f1;

				final String pathKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_MOUNT_PATH_KEY);
				final String subPathKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY);
				final String readOnlyKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_MOUNT_READONLY_KEY);

				return new KubernetesVolumeSpec(volumeName, properties.get(pathKey),
					properties.getOrDefault(subPathKey, ""),
					Boolean.parseBoolean(properties.getOrDefault(readOnlyKey, "false")),
					parseVolumeSpecificConf(properties, volumeType, volumeName));

			}).toArray(KubernetesVolumeSpec[]::new);
	}

	private static Set<Tuple2<String, String>> getVolumeTypesAndNames(Map<String, String> properties) {
		Set<Tuple2<String, String>> volumeTypesAndNames = new HashSet<>();
		properties.keySet().forEach(k -> {
			String[] tokens = k.split("\\.");
			if (tokens.length > 1) {
				volumeTypesAndNames.add(Tuple2.of(tokens[0], tokens[1]));
			}
		});

		return volumeTypesAndNames;
	}

	private static KubernetesVolumeSpecificConf parseVolumeSpecificConf(Map<String, String> properties, String volumeType, String volumeName) {
		switch (volumeType) {
			case KUBERNETES_VOLUMES_HOSTPATH_TYPE:
				final String pathKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_OPTIONS_PATH_KEY);
				return new KubernetesHostPathVolumeConf(properties.get(pathKey));

			case KUBERNETES_VOLUMES_PVC_TYPE:
				final String claimNameKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY);
				return new KubernetesPVCVolumeConf(properties.get(claimNameKey));

			case KUBERNETES_VOLUMES_EMPTYDIR_TYPE:
				final String mediumKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY);
				final String sizeLimitKey = String.join(".", volumeType, volumeName, KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY);
				return new KubernetesEmptyDirVolumeConf(Optional.ofNullable(properties.get(mediumKey)), Optional.ofNullable(properties.get(sizeLimitKey)));

			default:
				throw new IllegalArgumentException("Kubernetes Volume type "  + volumeType + " is not supported");
		}
	}

	public static Tuple2<Volume[], VolumeMount[]> constructVolumes(KubernetesVolumeSpec[] volumeSpecs) {
		final List<Volume> volumes = new ArrayList<>();
		final List<VolumeMount> volumeMounts = new ArrayList<>();

		Arrays.stream(volumeSpecs).forEach(spec -> {
			final VolumeMount volumeMount = new VolumeMountBuilder()
				.withMountPath(spec.getMountPath())
				.withSubPath(spec.getMountSubPath())
				.withReadOnly(spec.isMountReadOnly())
				.withName(spec.getVolumeName())
				.build();
			volumeMounts.add(volumeMount);

			final KubernetesVolumeSpecificConf kubernetesVolumeSpecificConf = spec.getVolumeSpecifConf();
			VolumeBuilder volumeBuilder;
			if (kubernetesVolumeSpecificConf instanceof KubernetesHostPathVolumeConf) {
				final KubernetesHostPathVolumeConf kubernetesHostPathVolumeConf = (KubernetesHostPathVolumeConf) kubernetesVolumeSpecificConf;
				volumeBuilder = new VolumeBuilder()
					.withHostPath(new HostPathVolumeSource(kubernetesHostPathVolumeConf.getHostPath(), ""));
			} else if (kubernetesVolumeSpecificConf instanceof KubernetesPVCVolumeConf) {
				final KubernetesPVCVolumeConf kubernetesPVCVolumeConf = (KubernetesPVCVolumeConf) kubernetesVolumeSpecificConf;
				volumeBuilder =  new VolumeBuilder()
					.withPersistentVolumeClaim(
						new PersistentVolumeClaimVolumeSource(kubernetesPVCVolumeConf.getClaimName(), spec.isMountReadOnly()));
			} else {
				final KubernetesEmptyDirVolumeConf kubernetesEmptyDirVolumeConf = (KubernetesEmptyDirVolumeConf) kubernetesVolumeSpecificConf;
				volumeBuilder = new VolumeBuilder()
					.withEmptyDir(new EmptyDirVolumeSource(kubernetesEmptyDirVolumeConf.getMedium().orElse(""),
						new Quantity(kubernetesEmptyDirVolumeConf.getSizeLimit().orElse(null))));
			}

			final Volume volume = volumeBuilder.withName(spec.getVolumeName()).build();
			volumes.add(volume);
		});

		return Tuple2.of(volumes.toArray(new Volume[0]), volumeMounts.toArray(new VolumeMount[0]));
	}

}
