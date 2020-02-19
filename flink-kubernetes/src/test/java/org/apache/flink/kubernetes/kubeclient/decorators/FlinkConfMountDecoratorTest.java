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

import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link FlinkConfMountDecorator}.
 */
public class FlinkConfMountDecoratorTest extends JobManagerDecoratorTestBase {
	private static final String INTERNAL_FLINK_CONF_DIR = "/opt/flink/flink-conf-";

	private FlinkConfMountDecorator flinkConfMountDecorator;

	@Before
	public void setup() throws Exception {
		super.setup();
		this.flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, INTERNAL_FLINK_CONF_DIR);
		this.flinkConfMountDecorator = new FlinkConfMountDecorator(kubernetesJobManagerParameters);
	}

	@Test
	public void testWhetherPodOrContainerIsDecorated() {
		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);
		assertNotEquals(baseFlinkPod.getPod(), decoratedFlinkPod.getPod());
		assertNotEquals(baseFlinkPod.getMainContainer(), decoratedFlinkPod.getMainContainer());
	}

	@Test
	public void testConfigMap() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final List<HasMetadata> additionalResources = flinkConfMountDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, additionalResources.size());

		final Map<String, String> expectedDatas = new HashMap<>();
		expectedDatas.put(FLINK_CONF_FILENAME, flinkConfMountDecorator.getFlinkConfData(this.flinkConfig));
		expectedDatas.put("logback.xml", "some data");
		expectedDatas.put("log4j.properties", "some data");
		final List<ConfigMap> expectedConfigMaps = Collections.singletonList(new ConfigMapBuilder()
			.withNewMetadata()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withLabels(kubernetesJobManagerParameters.getCommonLabels())
				.endMetadata()
			.addToData(expectedDatas)
			.build());
		assertThat(expectedConfigMaps, is(additionalResources));
	}

	@Test
	public void testDecorateFlinkPodWithoutFlinkConfigYAML() {
		// todo 当做一种异常场景去处理
		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);
	}

	@Test
	public void testDecoratedFlinkPodWithoutLog4jAndLogback() {
		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Collections.singletonList(
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
					.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
					.withItems(expectedKeyToPaths)
					.endConfigMap()
				.build());
		assertThat(expectedVolumes, is(decoratedFlinkPod.getPod().getSpec().getVolumes()));

		final List<VolumeMount> expectedVolumeMounts = Collections.singletonList(
			new VolumeMountBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withMountPath(INTERNAL_FLINK_CONF_DIR)
			.build());
		assertThat(expectedVolumeMounts, is(decoratedFlinkPod.getMainContainer().getVolumeMounts()));
	}

	@Test
	public void testDecoratedFlinkPodWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("log4j.properties")
				.withPath("log4j.properties")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertThat(expectedVolumes, is(decoratedFlinkPod.getPod().getSpec().getVolumes()));
	}

	@Test
	public void testDecoratedFlinkPodWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("logback.xml")
				.withPath("logback.xml")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertThat(expectedVolumes, is(decoratedFlinkPod.getPod().getSpec().getVolumes()));
	}

	@Test
	public void testDecoratedFlinkPodWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final FlinkPod decoratedFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("logback.xml")
				.withPath("logback.xml")
				.build(),
			new KeyToPathBuilder()
				.withKey("log4j.properties")
				.withPath("log4j.properties")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertThat(expectedVolumes, is(decoratedFlinkPod.getPod().getSpec().getVolumes()));
	}
}
