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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Base test class for Kubernetes.
 */
public class KubernetesTestBase extends TestLogger {

	protected static final String NAMESPACE = "test";
	protected static final String CLUSTER_ID = "my-flink-cluster1";
	protected static final String CONTAINER_IMAGE = "flink-k8s-test:latest";
	protected static final String CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

	@Rule
	public MixedKubernetesServer server = new MixedKubernetesServer(true, true);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	protected File flinkConfDir;

	protected final Configuration flinkConfig = new Configuration();

	protected KubernetesClient kubeClient;

	protected FlinkKubeClient flinkKubeClient;

	@Before
	public void setup() throws Exception {
		flinkConfig.setString(KubernetesConfigOptions.NAMESPACE, NAMESPACE);
		flinkConfig.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, CONTAINER_IMAGE_PULL_POLICY);

		flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		writeFlinkConfiguration();

		Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);

		kubeClient = server.getClient().inNamespace(NAMESPACE);
		flinkKubeClient = new Fabric8FlinkKubeClient(flinkConfig, kubeClient);
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(){
		return getFabric8FlinkKubeClient(flinkConfig);
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(Configuration flinkConfig){
		return new Fabric8FlinkKubeClient(flinkConfig, server.getClient().inNamespace(NAMESPACE));
	}

	protected KubernetesClient getKubeClient() {
		return server.getClient().inNamespace(NAMESPACE);
	}

	protected void writeFlinkConfiguration() throws IOException {
		BootstrapTools.writeConfiguration(this.flinkConfig, new File(flinkConfDir, "flink-conf.yaml"));
	}

	protected Map<String, String> getCommonLabels() {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, CLUSTER_ID);
		return labels;
	}

	protected void mockServiceAddEvent(String serviceName) {
		final Service mockService = new ServiceBuilder()
			.editOrNewMetadata()
			.endMetadata()
			.build();

		final String path = String.format("/api/v1/namespaces/%s/services?fieldSelector=metadata.name%%3D%s&watch=true",
			NAMESPACE, serviceName);

		server.expect()
			.withPath(path)
			.andUpgradeToWebSocket()
			.open()
			.waitFor(1000)
			.andEmit(new WatchEvent(mockService, "ADDED"))
			.done()
			.once();
	}
}
