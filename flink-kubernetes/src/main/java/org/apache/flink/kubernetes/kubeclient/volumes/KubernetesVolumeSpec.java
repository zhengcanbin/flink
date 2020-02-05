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

package org.apache.flink.kubernetes.kubeclient.volumes;

/**
 * Spec.
 */
public class KubernetesVolumeSpec {

	private final String volumeName;

	private final String mountPath;

	private final String mountSubPath;

	private final boolean mountReadOnly;

	private final KubernetesVolumeSpecificConf volumeSpecifConf;

	public KubernetesVolumeSpec(String volumeName, String mountPath, String mountSubPath,
								boolean mountReadOnly, KubernetesVolumeSpecificConf volumeSpecifConf) {
		this.volumeName = volumeName;
		this.mountPath = mountPath;
		this.mountSubPath = mountSubPath;
		this.mountReadOnly = mountReadOnly;
		this.volumeSpecifConf = volumeSpecifConf;
	}

	public String getVolumeName() {
		return volumeName;
	}

	public String getMountPath() {
		return mountPath;
	}

	public String getMountSubPath() {
		return mountSubPath;
	}

	public boolean isMountReadOnly() {
		return mountReadOnly;
	}

	public KubernetesVolumeSpecificConf getVolumeSpecifConf() {
		return volumeSpecifConf;
	}
}
