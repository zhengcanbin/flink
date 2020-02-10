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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.Objects;

/**
 *
 */
public class FlinkPod {

	private Pod pod;

	private Container mainContainer;

	public FlinkPod(Pod pod, Container mainContainer) {
		this.pod = pod;
		this.mainContainer = mainContainer;
	}

	public Pod getPod() {
		return pod;
	}

	public void setPod(Pod pod) {
		this.pod = pod;
	}

	public Container getMainContainer() {
		return mainContainer;
	}

	public void setMainContainer(Container mainContainer) {
		this.mainContainer = mainContainer;
	}


}
