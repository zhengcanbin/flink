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

import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

/**
 * Creates an external Service to expose the ports of the Flink JobManager(s).
 * This can include the rest port, and the blob server port.
 */
public class ExternalServiceDecorator extends AbstractServiceDecorator {

	public ExternalServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		super(kubernetesJobManagerParameters);
	}

	@Override
	protected String getServiceType() {
		return kubernetesJobManagerParameters.getRestServiceExposedType();
	}

	@Override
	protected boolean isRestPortOnly() {
		return true;
	}

	@Override
	protected String getServiceName() {
		return KubernetesUtils.getRestServiceName(kubernetesJobManagerParameters.getClusterId());
	}

	@Override
	protected boolean isInternalService() {
		return false;
	}
}
